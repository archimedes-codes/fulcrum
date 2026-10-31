//! (ADR 0030 Task 5) The OS sandbox a `Sandboxed` `bash` runs inside:
//! **Landlock**, installed in the CHILD only (via `pre_exec`) so it never
//! touches the Worker process, let alone the desktop.
//!
//! Why a sandbox at all: `sh -c` cannot be path-parsed (`cat $(echo
//! /etc/pa$$)`), so the only honest way to honour `Shell=Sandboxed` is to
//! confine the process the command runs in. Landlock is the Linux kernel's
//! own unprivileged sandbox: deny-by-default and ADD-ONLY (a confined
//! process can narrow its reach, never widen it), and reachable through raw
//! `libc` — but `libc` 0.2 ships ONLY the three syscall numbers
//! (`SYS_landlock_create_ruleset` / `_add_rule` / `_restrict_self`); every
//! struct, every access-right constant and the `PR_SET_NO_NEW_PRIVS` prctl
//! option are hand-rolled here (verified against libc 0.2.189, which has no
//! `landlock_*` type, constant or wrapper at all).
//!
//! # The access-right constants are a transcription — they are PINNED
//! Because `libc` ships no Landlock constants, `ACCESS_*` below is copied by
//! hand from the kernel UAPI header. A wrong bit does not fail loudly: the
//! ruleset still builds, and the sandbox silently enforces a DIFFERENT (and
//! much weaker) right set — which is exactly the bug this module used to
//! have. `the_access_right_table_matches_the_kernel_uapi_header` pins every
//! value against `/usr/include/linux/landlock.h` at test time, so the table
//! cannot drift again. Trust the header, never a comment in this file: the
//! previous table was mistranscribed AND its symptoms were rationalized in
//! the comments as "kernel behavior".
//!
//! # Handled rights: the kernel's real set, masked to the probed ABI
//! A right that is not HANDLED is not enforced at all, so the handled mask —
//! not the rules — is what makes the tier strict. `landlock_create_ruleset`
//! answers `EAGAIN` for a bit the kernel does not know, so the mask is
//! derived from the probed ABI version (see [`handled_access_for`]). The
//! kernel's own floors (from the UAPI docs, NOT from any assumption) are:
//! `REFER` ABI 2, `TRUNCATE` ABI 3, `IOCTL_DEV` ABI 5, `RESOLVE_UNIX` ABI 9.
//! * `READ_FILE` handled ⇒ reads really are confined (it used to be absent,
//!   which made every read outside the allow-list work).
//! * `TRUNCATE` handled + granted inside the write boundary ⇒ `O_TRUNC` and
//!   `truncate(2)` still work there and are denied outside. Without it,
//!   `echo x > existing_file` breaks.
//! * `REFER` handled + granted on the write roots ⇒ cross-directory `mv` /
//!   hardlink inside the boundary keeps working (it is "always denied by
//!   default", header line 25, so handling it without granting it would
//!   break `mv a/f b/`).
//! * `IOCTL_DEV` handled, granted only on the entropy/nullness nodes we
//!   rule ⇒ a confined child cannot `ioctl` an arbitrary device
//!   (`/dev/sd*`, and NOT the controlling terminal: `/dev/tty` is
//!   deliberately not write-ruled at all, see [`READ_WRITE_DEVICES`]).
//! * `RESOLVE_UNIX` is deliberately NOT handled. Handling it would deny
//!   connecting to UNIX sockets created outside the domain (docker.sock, the
//!   systemd bus — measured) while buying nothing, because this tier does
//!   not restrict TCP at all (see "Honest scope"): a confined command can
//!   still reach the docker daemon over `127.0.0.1`. Enabling it later must
//!   come with network rules, not on its own.
//!
//! # Rules must be masked to the fd's type
//! A `PATH_BENEATH` rule whose mask contains a right that cannot apply to
//! the fd's type is rejected `EINVAL` — and a rejected rule is a rule the
//! sandbox does NOT have. Directory-only rights (`READ_DIR`, `REMOVE_*`,
//! `MAKE_*`) on a file fd, and `IOCTL_DEV` on a non-device fd, are masked
//! out in [`mask_for_fd`] (measured; the old code's "`READ_FILE | EXECUTE` on
//! a non-directory fd is rejected `EINVAL`" comment was this same mixup
//! talking — that pair is really `{EXECUTE, READ_DIR}`, and `READ_DIR` on a
//! file is the `EINVAL`). Rule paths are canonicalized first: `O_PATH` on a
//! symlink yields an fd the kernel refuses to rule, so a rule on e.g.
//! `/etc/resolv.conf` (a symlink into `/run`) silently did nothing.
//!
//! # The ruleset
//! * read + execute: `/usr`, `/bin`, `/dev`, `/proc` (REQUIRED — without
//!   them the dynamic linker cannot load anything and no command runs),
//!   plus `/lib`, `/lib64`, `/sbin` where they exist (distro-dependent
//!   layouts: Fedora symlinks `/lib64` into `/usr`, a Debian tier has no
//!   `/lib64`; a missing one is skipped, not an error).
//! * read a named list of `/etc` FILES (the loader cache, the resolver,
//!   `passwd`/`group`, the CA bundle dirs): the resolver and TLS are dead
//!   without them and `whoami`/`git` are useless without `passwd`. These are
//!   `READ_FILE`-only rules, so the files are readable but the directories
//!   are NOT listable.
//! * read + write: the nullness/entropy nodes under `/dev` (`/dev/null` and
//!   friends — `git --version` fails without `/dev/null` read-write), and
//!   [`crate::agent::boundary::write_roots`] (the canonical session `cwd`) +
//!   `/tmp` + `/var/tmp`, MINUS the protected agent-definition dirs (below).
//!
//! # The protected dirs ARE subtracted (Landlock cannot subtract, so we
//! enumerate)
//! A `PATH_BENEATH` rule grants its rights on the fd's path AND on
//! everything beneath it, and the ruleset is the UNION of the rules: there
//! is no narrower rule that takes a right back from an ancestor. So a write
//! root that merely CONTAINS a protected dir cannot be ruled read-write.
//! Instead ([`rule_write_root`]): the root itself is ruled read+execute and
//! every SIBLING along the path down to the protected dir is ruled
//! read-write, so the tree stays writable everywhere except the protected
//! subtree. The cost, in exchange for the protection: when the session
//! `cwd` is an ancestor of a protected dir (e.g. a session rooted at
//! `$HOME`), files cannot be CREATED directly in `cwd` itself (only in its
//! subdirectories) — Landlock simply cannot express both. And a `cwd` that
//! is inside a protected dir gets NO write rights at all: that is the case
//! the write tools' deny-list exists for, and it must hold at the tightest
//! tier too.
//!
//! # Honest scope — read this before calling it isolation
//! The ruleset confines the child's reach into YOUR FILES. It is not a
//! confidentiality boundary and not a VM:
//! * The system directories are readable BY DESIGN (a `bash` that cannot
//!   `ld.so` is not a usable shell), and the `/etc` files listed above plus
//!   the CA bundle are deliberately readable.
//! * This tier enables NO network rules AT ANY ABI (`handled_access_net`
//!   is 0; Landlock's own network rights exist from ABI 4 for TCP and ABI
//!   10 for UDP, and are not used here): a confined command can still
//!   `curl` out with anything it was allowed to read.
//! * It is TIGHTER than the write tools' `Allow` floor (ADR 0030 Deviation
//!   3) — Landlock is allow-list-only, so "anywhere except the agent dirs"
//!   is not directly expressible; the protected dirs are subtracted by the
//!   enumeration described above, which is a real enforcement, not an
//!   accident of the allow-list.
//!
//! # Fail closed
//! No ruleset means no run: the spawn fails and `exec_bash` surfaces the
//! reason (`"Sandboxed shell is unavailable: <reason>"`; off Linux, where
//! this module is not compiled at all, `"… on this platform"`). A run that
//! silently escaped would break exactly the promise the user selected.

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

// ── The hand-rolled uapi types (`libc` has none) ─────────────────────────

/// `struct landlock_ruleset_attr`. ABI 1 has exactly one field (the kernel
/// accepts a SMALLER size argument, so one field is forward-compatible).
#[repr(C)]
struct LandlockRulesetAttr {
    /// The access rights the ruleset HANDLES: everything OUTSIDE this mask
    /// stays unrestricted, so this mask is what makes the sandbox strict.
    handled_access_fs: u64,
}

/// `struct landlock_path_beneath_attr` — one rule: these rights, beneath
/// the directory `parent_fd` names.
#[repr(C)]
struct LandlockPathBeneathAttr {
    /// The rights granted beneath `parent_fd` (a subset of the handled set).
    allowed_access: u64,
    /// A directory fd, opened `O_PATH | O_CLOEXEC` in the PARENT.
    parent_fd: i32,
}

// ── The filesystem access rights (`LANDLOCK_ACCESS_FS_*`) ────────────────
// Transcribed from the kernel UAPI (`include/uapi/linux/landlock.h`, mirrored
// by `/usr/include/linux/landlock.h`). THE VALUES ARE THE ABI — a wrong bit
// is not a typo, it is a different (weaker) sandbox. Pinned by
// `the_access_right_table_matches_the_kernel_uapi_header`.
const ACCESS_EXECUTE: u64 = 1 << 0;
const ACCESS_WRITE_FILE: u64 = 1 << 1;
const ACCESS_READ_FILE: u64 = 1 << 2;
const ACCESS_READ_DIR: u64 = 1 << 3;
const ACCESS_REMOVE_DIR: u64 = 1 << 4;
const ACCESS_REMOVE_FILE: u64 = 1 << 5;
const ACCESS_MAKE_CHAR: u64 = 1 << 6;
const ACCESS_MAKE_DIR: u64 = 1 << 7;
const ACCESS_MAKE_REG: u64 = 1 << 8;
const ACCESS_MAKE_SOCK: u64 = 1 << 9;
const ACCESS_MAKE_FIFO: u64 = 1 << 10;
const ACCESS_MAKE_BLOCK: u64 = 1 << 11;
const ACCESS_MAKE_SYM: u64 = 1 << 12;
const ACCESS_REFER: u64 = 1 << 13;
const ACCESS_TRUNCATE: u64 = 1 << 14;
const ACCESS_IOCTL_DEV: u64 = 1 << 15;
const ACCESS_RESOLVE_UNIX: u64 = 1 << 16;

/// Every right this module knows about, with the MINIMUM Landlock ABI
/// version that supports it (from the UAPI docs: `REFER` 2, `TRUNCATE` 3,
/// `IOCTL_DEV` 5, `RESOLVE_UNIX` 9).
const ACCESS_TABLE: &[(u64, u32)] = &[
    (ACCESS_EXECUTE, 1),
    (ACCESS_WRITE_FILE, 1),
    (ACCESS_READ_FILE, 1),
    (ACCESS_READ_DIR, 1),
    (ACCESS_REMOVE_DIR, 1),
    (ACCESS_REMOVE_FILE, 1),
    (ACCESS_MAKE_CHAR, 1),
    (ACCESS_MAKE_DIR, 1),
    (ACCESS_MAKE_REG, 1),
    (ACCESS_MAKE_SOCK, 1),
    (ACCESS_MAKE_FIFO, 1),
    (ACCESS_MAKE_BLOCK, 1),
    (ACCESS_MAKE_SYM, 1),
    (ACCESS_REFER, 2),
    (ACCESS_TRUNCATE, 3),
    (ACCESS_IOCTL_DEV, 5),
    (ACCESS_RESOLVE_UNIX, 9),
];

/// The rights that only apply to a DIRECTORY fd. On any other fd the kernel
/// rejects the whole rule `EINVAL` (see [`mask_for_fd`]).
const DIR_ONLY_ACCESS: u64 = ACCESS_READ_DIR
    | ACCESS_REMOVE_DIR
    | ACCESS_REMOVE_FILE
    | ACCESS_MAKE_CHAR
    | ACCESS_MAKE_DIR
    | ACCESS_MAKE_REG
    | ACCESS_MAKE_SOCK
    | ACCESS_MAKE_FIFO
    | ACCESS_MAKE_BLOCK
    | ACCESS_MAKE_SYM;

/// The rights the ruleset would like to handle (the kernel's mask is
/// [`handled_access_for`]).
const ALL_KNOWN_ACCESS: u64 = ACCESS_EXECUTE
    | ACCESS_WRITE_FILE
    | ACCESS_READ_FILE
    | ACCESS_READ_DIR
    | ACCESS_REMOVE_DIR
    | ACCESS_REMOVE_FILE
    | ACCESS_MAKE_CHAR
    | ACCESS_MAKE_DIR
    | ACCESS_MAKE_REG
    | ACCESS_MAKE_SOCK
    | ACCESS_MAKE_FIFO
    | ACCESS_MAKE_BLOCK
    | ACCESS_MAKE_SYM
    | ACCESS_REFER
    | ACCESS_TRUNCATE
    | ACCESS_IOCTL_DEV
    | ACCESS_RESOLVE_UNIX;

/// The handled mask for a kernel of this ABI version: a bit the kernel does
/// not know makes `landlock_create_ruleset` fail `EAGAIN`, so the mask MUST
/// be derived from the probe. `RESOLVE_UNIX` is excluded on purpose (module
/// doc: it would break docker/systemd UNIX sockets while TCP stays open).
fn handled_access_for(abi: u32) -> u64 {
    ACCESS_TABLE
        .iter()
        .filter(|(_, min_abi)| *min_abi <= abi)
        .map(|(bit, _)| *bit)
        .fold(0, |mask, bit| mask | bit)
        & !ACCESS_RESOLVE_UNIX
}

/// Read + execute: run a binary, read its data, traverse the tree — no write
/// rights at all (`WRITE_FILE` is what makes `/proc/self/…` writable, so it
/// is NOT in this mask; see `proc_is_read_execute_only_and_stays_listable`).
const READ_EXECUTE: u64 = ACCESS_EXECUTE | ACCESS_READ_FILE | ACCESS_READ_DIR;
/// The full working-tree set inside the write boundary: create, overwrite
/// (`WRITE_FILE` + `TRUNCATE`), unlink, rename and link (`REMOVE_*` +
/// `MAKE_*` + `REFER`), plus the device `ioctl`s where the fd is a device.
const READ_WRITE: u64 = ALL_KNOWN_ACCESS & !ACCESS_RESOLVE_UNIX;

/// `LANDLOCK_CREATE_RULESET_VERSION`: ask for the kernel's ABI instead of
/// building a ruleset (the availability probe — a NULL attr with size 0).
const CREATE_RULESET_VERSION: u32 = 1 << 0;
/// `enum landlock_rule_type` — the only filesystem rule kind.
const RULE_PATH_BENEATH: u32 = 1;
/// `PR_SET_NO_NEW_PRIVS`. `libc::prctl` IS reachable on linux-gnu but this
/// constant is NOT defined there, and `landlock_restrict_self` returns
/// `EPERM` unless it was set first (the kernel refuses to confine a process
/// that could still gain privileges, e.g. through a setuid binary).
const PR_SET_NO_NEW_PRIVS: i32 = 38;

/// The read+execute dirs without which NOTHING runs (the loader, the shell
/// itself, `/dev/null`, `/proc/self`): failing to rule one of these fails
/// closed rather than producing a half-ruleset.
const REQUIRED_READ_DIRS: &[&str] = &["/usr", "/bin", "/dev", "/proc"];
/// Distro-dependent equivalents / aliases — skipped when absent.
const OPTIONAL_READ_DIRS: &[&str] = &["/lib", "/lib64", "/sbin"];
/// The character devices ordinary commands need READ + WRITE on (a shell
/// redirects to `/dev/null`; `git --version` fails outright without it).
/// `IOCTL_DEV` is granted here too — harmless on these nodes, which expose
/// no terminal interface (the child's own stdio is PIPED, so it needs no
/// terminal `ioctl` at all).
///
/// `/dev/tty` is deliberately ABSENT. `exec.rs` spawns the child with
/// `process_group(0)` — which does not detach the controlling terminal — and
/// pipes only stdout/stderr, so a confined child STILL holds the Worker's
/// ctty. Granting `WRITE_FILE` there would let a sandboxed command write raw
/// escape sequences to the user's terminal, and `IOCTL_DEV` would allow
/// `TIOCSTI` keystroke injection on kernels < 6.2. The grant buys nothing
/// (stdio is piped), so it is not granted. A READ grant is not needed
/// either: `/dev` is already ruled read+execute — which is why removing it
/// from this list does not make `/dev/tty` unreadable, only unwriteable.
const READ_WRITE_DEVICES: &[&str] = &[
    "/dev/null",
    "/dev/zero",
    "/dev/full",
    "/dev/random",
    "/dev/urandom",
];
/// The temp dirs ordinary commands require (`mktemp`, `tar`, `git`).
const TEMP_DIRS: &[&str] = &["/tmp", "/var/tmp"];
/// Single files a confined shell must be able to READ: the loader cache, the
/// resolver (`/etc/resolv.conf` is a symlink into `/run`, which is why rule
/// paths are canonicalized), the identity files (`whoami`/`id`), the CA
/// trust anchor list, and git's config files (without them `git status`
/// exits 128 the moment `$HOME` holds a `.gitconfig`, which is the normal
/// case). All OPTIONAL: a distro without one still runs (glibc falls back to
/// its default search paths without the loader cache, `musl` has no
/// `nsswitch.conf`, Arch has no `/etc/gitconfig`).
const OPTIONAL_READ_FILES: &[&str] = &[
    "/etc/ld.so.cache",
    "/etc/resolv.conf",
    "/etc/nsswitch.conf",
    "/etc/hosts",
    "/etc/localtime",
    "/etc/passwd",
    "/etc/group",
    "/etc/gitconfig",
    "/etc/machine-id",
];
/// The CA trust roots. `READ_FILE` on a DIRECTORY rule makes every file
/// beneath it readable while the directories themselves stay UNLISTABLE, so
/// this is a narrow grant, not `/etc` being readable. Distro layouts differ
/// (Fedora `/etc/pki/tls/certs`, Debian `/etc/ssl/certs`), hence optional.
const OPTIONAL_CA_DIRS: &[&str] = &["/etc/pki", "/etc/ssl", "/etc/crypto-policies"];
/// Git's per-user config, read WITHOUT making `$HOME` listable: git refuses
/// to run (`exit 128`) when `$HOME/.gitconfig` exists and cannot be read, so
/// a confined shell could not run `git status` in its own repo without this.
fn git_config_files() -> Vec<PathBuf> {
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return Vec::new();
    };
    vec![
        home.join(".gitconfig"),
        home.join(".config/git/config"),
        home.join(".config/git/ignore"),
    ]
}

// ── The syscalls (raw `libc::syscall` — libc has no landlock wrappers) ───

/// `landlock_create_ruleset(2)`.
unsafe fn create_ruleset(attr: *const LandlockRulesetAttr, size: usize, flags: u32) -> i32 {
    // SAFETY: the syscall number comes from `libc`. With the VERSION flag
    // the kernel reads no attr at all; otherwise `attr` points to a live
    // `LandlockRulesetAttr` of exactly `size` bytes. All arguments are
    // passed by value (no pointer escapes into the child).
    unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            attr as usize,
            size,
            flags as usize,
        ) as i32
    }
}

/// `landlock_add_rule(2)` (flags = 0, the only defined value).
unsafe fn add_rule(ruleset_fd: i32, rule_type: u32, rule: *const LandlockPathBeneathAttr) -> i32 {
    // SAFETY: `ruleset_fd` is a live ruleset fd and `rule` points to a live
    // attr whose `parent_fd` is a live directory fd; the caller owns both
    // for the duration of the call.
    unsafe {
        libc::syscall(
            libc::SYS_landlock_add_rule,
            ruleset_fd as usize,
            rule_type as usize,
            rule as usize,
            0usize,
        ) as i32
    }
}

/// `landlock_restrict_self(2)` — applies the ruleset to the CALLING thread.
/// That is precisely why it runs from `pre_exec`: the child is confined and
/// nothing else (no Worker, no desktop).
unsafe fn restrict_self(ruleset_fd: i32) -> i32 {
    // SAFETY: `ruleset_fd` is a live ruleset fd owned by the caller.
    unsafe {
        libc::syscall(
            libc::SYS_landlock_restrict_self,
            ruleset_fd as usize,
            0usize,
        ) as i32
    }
}

/// The kernel's Landlock ABI version (`> 0` = Landlock is available).
fn ruleset_abi_version() -> i32 {
    // SAFETY: with `LANDLOCK_CREATE_RULESET_VERSION` the kernel reads no
    // attr and requires size 0.
    unsafe { create_ruleset(std::ptr::null(), 0, CREATE_RULESET_VERSION) }
}

/// Whether this kernel can enforce a Landlock ruleset — the Settings
/// page's grey-out source AND the `exec_bash` fail-closed check. `false`
/// means `Sandboxed` commands REFUSE to run; it never means they run
/// unsandboxed.
pub fn landlock_available() -> bool {
    ruleset_abi_version() > 0
}

// ── The ruleset ──────────────────────────────────────────────────────────

/// A built ruleset, held as its fd. All parent-side work happens in
/// [`Sandbox::create`]; [`Sandbox::install`] then hands the SAME fd to the
/// child's `pre_exec` closure (`fork` duplicates the descriptor table, so
/// the number refers to the same open ruleset in the child, and
/// `restrict_self` consumes it there).
///
/// `Drop` closes the fd in the PARENT — the child's copy is a separate
/// descriptor, so dropping the guard after `spawn` is correct.
pub struct Sandbox {
    /// The ruleset fd (owned: closed exactly once, on drop).
    ruleset_fd: i32,
    /// The rights the ruleset HANDLES. A rule that grants a right outside
    /// this mask is rejected `EINVAL` — so EVERY grant is intersected with
    /// it (see [`Sandbox::add_path`]). Without that, an ABI-1 kernel (5.13,
    /// the floor this tier claims to support) would reject the whole
    /// read-write rule for the session directory because `READ_WRITE` also
    /// names `REFER` / `TRUNCATE` / `IOCTL_DEV`.
    handled: u64,
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        // SAFETY: the fd is ours by construction and this type is not
        // `Clone`/`Copy`, so it is closed exactly once.
        unsafe { libc::close(self.ruleset_fd) };
    }
}

/// Open one rule's path. `O_PATH` needs no read permission on the target
/// (a root-owned system dir can still be ruled) and `O_CLOEXEC` keeps the
/// descriptor from leaking into the sandboxed command's own fd table —
/// which matters, because an inherited fd bypasses the ruleset.
///
/// The path is CANONICALIZED first. `O_PATH` on a symlink yields an fd the
/// kernel refuses to rule (`EBADF`), so a rule written against a symlinked
/// path — `/etc/resolv.conf` into `/run`, `/lib64` into `/usr` — would be
/// dropped in silence. Canonicalizing also means the rule follows the real
/// object the child will reach through that path.
fn open_rule_path(path: &Path) -> Result<i32, String> {
    let canonical = path
        .canonicalize()
        .map_err(|e| format!("cannot resolve {} ({})", path.display(), e))?;
    let name = CString::new(canonical.as_os_str().as_bytes())
        .map_err(|_| format!("{} contains a NUL byte", canonical.display()))?;
    // SAFETY: `name` is a valid NUL-terminated C string; the mode argument
    // is ignored because no create flag is set.
    let fd = unsafe { libc::open(name.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(format!(
            "cannot open {} ({})",
            canonical.display(),
            std::io::Error::last_os_error()
        ));
    }
    Ok(fd)
}

/// Drop the rights that cannot apply to this fd's type, because the kernel
/// rejects the WHOLE RULE (`EINVAL`) rather than ignoring the odd bit — and a
/// rejected rule is a right the sandbox silently does not have. Measured on
/// ABI 10: every directory-only right on a file fd is `EINVAL`, and
/// `IOCTL_DEV` is meaningless (and refused) on a non-device fd.
fn mask_for_fd(fd: i32, access: u64) -> u64 {
    // SAFETY: `fd` is a live fd owned by the caller; `stat` only reads it.
    let st = {
        let mut st = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(fd, st.as_mut_ptr()) } != 0 {
            return access & !DIR_ONLY_ACCESS & !ACCESS_IOCTL_DEV;
        }
        // SAFETY: `fstat` succeeded, so the struct is initialized.
        unsafe { st.assume_init() }
    };
    let mode = st.st_mode;
    let mut mask = access;
    if mode & libc::S_IFMT != libc::S_IFDIR {
        mask &= !DIR_ONLY_ACCESS;
    }
    if mode & libc::S_IFMT != libc::S_IFCHR && mode & libc::S_IFMT != libc::S_IFBLK {
        mask &= !ACCESS_IOCTL_DEV;
    }
    mask
}

impl Sandbox {
    /// Build the ruleset for a session rooted at `cwd`. The write boundary
    /// is [`crate::agent::boundary::write_roots`] MINUS
    /// [`crate::agent::boundary::protected_dirs`], so a `cwd` that cannot be
    /// canonicalized is a fail-closed error (the same rule the `FsBackend`
    /// follows) and a `cwd` inside an agent-definition dir is read-only.
    ///
    /// Every fd this opens is opened AND closed here, in the parent: the
    /// `pre_exec` closure then does nothing but two syscalls, which is what
    /// makes it legal to run between `fork` and `exec`.
    pub fn create(cwd: &Path) -> Result<Self, String> {
        let abi = ruleset_abi_version();
        if abi <= 0 {
            return Err(format!("this kernel has no Landlock (ABI {abi})"));
        }
        let handled = handled_access_for(abi as u32);
        let attr = LandlockRulesetAttr {
            handled_access_fs: handled,
        };
        // SAFETY: `attr` is a live `LandlockRulesetAttr` of the given size.
        let ruleset_fd =
            unsafe { create_ruleset(&attr, std::mem::size_of::<LandlockRulesetAttr>(), 0) };
        if ruleset_fd < 0 {
            return Err(format!(
                "cannot create the Landlock ruleset ({})",
                std::io::Error::last_os_error()
            ));
        }
        let mut sandbox = Sandbox {
            ruleset_fd,
            handled,
        };
        for dir in REQUIRED_READ_DIRS {
            sandbox.add_path(Path::new(dir), READ_EXECUTE, true)?;
        }
        for dir in OPTIONAL_READ_DIRS {
            sandbox.add_path(Path::new(dir), READ_EXECUTE, false)?;
        }
        // The nullness/entropy nodes: read + write (+ `ioctl`, which these
        // nodes expose no terminal interface for). NOT `/dev/tty` — that is
        // the user's terminal, see [`READ_WRITE_DEVICES`].
        for node in READ_WRITE_DEVICES {
            sandbox.add_path(
                Path::new(node),
                ACCESS_READ_FILE | ACCESS_WRITE_FILE | ACCESS_IOCTL_DEV,
                false,
            )?;
        }
        // The `/etc` files and the CA roots (a confined shell cannot resolve
        // a name, verify a certificate or look up its own user without them).
        for file in OPTIONAL_READ_FILES {
            sandbox.add_path(Path::new(file), ACCESS_READ_FILE, false)?;
        }
        for file in git_config_files() {
            sandbox.add_path(&file, ACCESS_READ_FILE, false)?;
        }
        for dir in OPTIONAL_CA_DIRS {
            sandbox.add_path(Path::new(dir), ACCESS_READ_FILE, false)?;
        }
        // The write boundary (minus the protected dirs) + the temp dirs.
        let write_roots = crate::agent::boundary::write_roots(cwd);
        if write_roots.is_empty() {
            return Err(format!(
                "the session boundary {} cannot be canonicalized",
                cwd.display()
            ));
        }
        let protected: Vec<PathBuf> = crate::agent::boundary::protected_dirs();
        for dir in write_roots {
            sandbox.rule_write_root(&dir, &protected, true)?;
        }
        for tmp in TEMP_DIRS {
            sandbox.rule_write_root(&PathBuf::from(tmp), &protected, false)?;
        }
        Ok(sandbox)
    }

    /// Make one write root writable EXCEPT inside the protected dirs.
    ///
    /// Landlock rules are additive (`PATH_BENEATH` covers the fd's path and
    /// everything under it, and the ruleset is the union of the rules), so
    /// there is no way to grant write on a directory and take it back for a
    /// subtree. Three cases:
    /// * the root IS, or sits INSIDE, a protected dir → read+execute only.
    ///   This is the exploit the write tools' deny-list exists to stop (a
    ///   session whose `cwd` is `~/.agents/skills` rewriting a `SKILL.md`),
    ///   so it holds at the tightest tier too.
    /// * no protected dir beneath it → the full read-write mask.
    /// * a protected subtree sits under it → read+execute on the chain of
    ///   directories down to it, read-write on every sibling along that
    ///   chain. The tree stays writable everywhere but the protected
    ///   subtree; the price is that files cannot be CREATED directly in an
    ///   ancestor on that chain (Landlock cannot express both).
    fn rule_write_root(
        &mut self,
        root: &Path,
        protected: &[PathBuf],
        required: bool,
    ) -> Result<(), String> {
        if protected.iter().any(|p| root == p || root.starts_with(p)) {
            return self.add_path(root, READ_EXECUTE, required);
        }
        // Compare CANONICAL paths: `protected_dirs` is canonicalized, and a
        // write root that reaches a protected dir through a symlink is just
        // as protected (the child reaches the same inode).
        let canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        if protected.iter().any(|p| canonical.starts_with(p)) {
            return self.add_path(root, READ_EXECUTE, required);
        }
        // (2) Nothing to carve out (the common case): full read-write.
        if !protected.iter().any(|p| p.starts_with(&canonical)) {
            return self.add_path(root, READ_WRITE, required);
        }
        // A protected dir lives somewhere under `root`: walk the chain and
        // rule every sibling read-write.
        self.rule_root_around_protected(&canonical, protected, required)
    }

    /// The recursive half of [`Sandbox::rule_write_root`]: `dir` contains a
    /// protected dir beneath it, so `dir` itself gets no write rights and
    /// its children are ruled individually.
    fn rule_root_around_protected(
        &mut self,
        dir: &Path,
        protected: &[PathBuf],
        required: bool,
    ) -> Result<(), String> {
        // Read + execute on the ancestor: write rights here would reach
        // inside the protected subtree (`MAKE_*`/`REMOVE_*` apply to entries
        // of any directory the right is granted on, beneath it).
        self.add_path(dir, READ_EXECUTE, required)?;
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            // Unreadable: nothing more can be carved out, and `dir` keeps
            // only read+execute, so this fails closed.
            Err(e) => {
                crate::agent::debuglog::log(&format!(
                    "sandbox: cannot list {} ({}) — it stays read-only",
                    dir.display(),
                    e
                ));
                return Ok(());
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // A symlinked child is NOT ruled: the rule would follow it and
            // grant write on whatever it points at, which may be anywhere.
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if protected.iter().any(|p| path == *p || path.starts_with(p)) {
                // The protected subtree itself: never writable.
                self.add_path(&path, READ_EXECUTE, false)?;
            } else if protected.iter().any(|p| path != *p && p.starts_with(&path)) {
                self.rule_root_around_protected(&path, protected, required)?;
            } else {
                self.add_path(&path, READ_WRITE, false)?;
            }
        }
        Ok(())
    }

    /// Add one rule, closing the path fd either way. A `required` path that
    /// cannot be opened or ruled is an error — a ruleset missing `/usr`
    /// would leave the child unable to exec ANYTHING, and a silently
    /// half-built ruleset is the opposite of fail closed. An optional one
    /// is skipped with a debug note (distro-dependent paths).
    fn add_path(&mut self, path: &Path, access: u64, required: bool) -> Result<(), String> {
        let skip = |e: String| {
            crate::agent::debuglog::log(&format!("sandbox: skip rule {e}"));
            Ok(())
        };
        let fd = match open_rule_path(path) {
            Ok(fd) => fd,
            Err(e) => return if required { Err(e) } else { skip(e) },
        };
        let rule = LandlockPathBeneathAttr {
            // The grant must be a SUBSET of the handled mask, or the kernel
            // refuses the entire rule (see `Sandbox::handled`).
            allowed_access: mask_for_fd(fd, access) & self.handled,
            parent_fd: fd,
        };
        // SAFETY: `self.ruleset_fd` is ours and `rule` refers to the fd
        // opened immediately above, which is closed after this call.
        let rc = unsafe { add_rule(self.ruleset_fd, RULE_PATH_BENEATH, &rule) };
        // SAFETY: `fd` is ours (opened above) and closed exactly once here.
        unsafe { libc::close(fd) };
        if rc < 0 {
            let err = format!(
                "cannot add a Landlock rule for {} ({})",
                path.display(),
                std::io::Error::last_os_error()
            );
            if required {
                return Err(err);
            }
            return skip(err);
        }
        Ok(())
    }

    /// Register the child-side install step on `cmd` (the confinement
    /// itself happens in the child, after `fork`, before `exec`).
    ///
    /// The closure captures the ruleset NUMBER and nothing else, which is
    /// sound: `fork` duplicates the descriptor table, so the number refers
    /// to the same open ruleset in the child, and the caller's [`Sandbox`]
    /// guard keeps the parent's fd alive until after `spawn`. `i32` is
    /// `Copy + Send + Sync`, which is what `pre_exec` requires (tokio runs
    /// the spawn on a blocking thread, so the closure must be `Send`).
    ///
    /// Registering a closure ALSO forces std's `fork`+`exec` path (std
    /// refuses `posix_spawn` when closures are registered) — which is how
    /// `process_group(0)` survives: std applies the process group in the
    /// child BEFORE running the closures, so the child is still a group
    /// leader and the negative-pid group kill in `exec.rs` still reaps
    /// backgrounded grandchildren.
    pub fn install(&self, cmd: &mut tokio::process::Command) {
        let ruleset_fd = self.ruleset_fd;
        // SAFETY: `pre_exec` is `unsafe` because the closure runs between
        // `fork` and `exec` in a multithreaded process, where only
        // async-signal-safe work is permitted. This closure performs TWO
        // syscalls and nothing else — no allocation, no path resolution, no
        // libc state, no locks. On failure it returns `io::Error`, which
        // makes `spawn` fail: fail closed, no command runs unsandboxed.
        unsafe {
            cmd.pre_exec(move || {
                // The order is required: `landlock_restrict_self` answers
                // `EPERM` unless no-new-privs is set first. Both calls take
                // only integer arguments (no pointer dereference at all).
                if libc::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // See `restrict_self` (the fd is alive — the guard outlives
                // the spawn in the caller).
                if restrict_self(ruleset_fd) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            })
        };
    }
}

#[cfg(test)]
mod tests;
