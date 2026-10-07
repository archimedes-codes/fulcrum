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
mod tests {
    use super::*;
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    use crate::agent::tools::{execute_tool, ContentBlock, ToolCtx, ToolResult};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    // ── skip visibility ──────────────────────────────────────────────────

    /// How many behavioral tests SKIPPED (no Landlock / no `/dev/shm`). A
    /// green suite where every confinement test skipped proves nothing, so a
    /// skip prints the greppable `SKIP-SANDBOX:` marker and this counts them;
    /// `sandbox_coverage_is_reported` prints the verdict line. On a kernel
    /// that confines, `grep -c SKIP-SANDBOX` over the run output MUST be 0.
    static SKIPPED: AtomicUsize = AtomicUsize::new(0);

    /// The base dir for the behavioral tests: `/dev/shm`, deliberately NOT
    /// under `/tmp` / `/var/tmp` — BOTH of those are in the sandbox
    /// allow-list (ordinary commands need temp dirs), so a test rooted
    /// there would pass VACUOUSLY. `/dev/shm` sits under `/dev`, which a
    /// confined child may read but not write, so a dir there is genuinely
    /// OUTSIDE the write boundary while still being writable by the
    /// (unconfined) test process.
    fn test_base() -> Option<PathBuf> {
        let base = Path::new("/dev/shm");
        base.is_dir().then(|| base.to_path_buf())
    }

    /// A file the TEST process owns (so `EACCES` proves Landlock did it, not
    /// DAC) that is OUTSIDE the sandbox's READ allow-list — the read tests
    /// cannot use [`test_base`], because `/dev/shm` sits under `/dev`, which
    /// the ruleset grants READ on by design (a shell that cannot reach
    /// `/dev/null` is not a shell). Candidates are the user's own runtime dir
    /// and `$HOME`; each is checked for being (a) ours, (b) writable, and (c)
    /// not under any directory the ruleset makes readable. `None` = no such
    /// place on this machine, so the caller skips loudly.
    fn private_dir_outside_the_read_allow_list() -> Option<PathBuf> {
        const READABLE: &[&str] = &[
            "/usr", "/bin", "/sbin", "/lib", "/lib64", "/dev", "/proc", "/etc", "/tmp", "/var/tmp",
        ];
        let candidates: Vec<PathBuf> = [
            std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
            Some(PathBuf::from(format!("/run/user/{}", unsafe {
                libc::getuid()
            }))),
            std::env::var_os("HOME").map(PathBuf::from),
        ]
        .into_iter()
        .flatten()
        .filter_map(|p| p.canonicalize().ok())
        .filter(|p| p.is_dir() && READABLE.iter().all(|r| !p.starts_with(r)))
        .collect();
        candidates.into_iter().find(|p| {
            // Writable by us, and readable by us (DAC): the file we plant
            // must be one the UNCONFINED process can read, or the confined
            // denial would prove nothing.
            let probe = p.join(format!("archimedes-sandbox-probe-{}", uuid::Uuid::new_v4()));
            let ok = std::fs::write(&probe, b"x").is_ok()
                && std::fs::read(&probe).map(|b| b == b"x").unwrap_or(false);
            let _ = std::fs::remove_file(&probe);
            ok
        })
    }

    /// SKIP (never fail) when the kernel has no Landlock or `/dev/shm` is
    /// absent: CI must stay reproducible on a kernel without Landlock, so
    /// these assert only behavior the running kernel can exhibit. The
    /// fail-CLOSED path is asserted unconditionally elsewhere (the exec.rs
    /// confined tests + the availability test below), and the skip is
    /// LOUD (marker + counter) so green cannot be mistaken for verified.
    macro_rules! require_sandbox {
        () => {{
            match test_base() {
                Some(base) if landlock_available() => base,
                _ => {
                    let n = SKIPPED.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    eprintln!(
                        "SKIP-SANDBOX: {} (no Landlock / no /dev/shm) — skip #{n}",
                        stringify!(file!()
                            : line!())
                    );
                    return;
                }
            }
        }};
    }

    /// A fresh dir under [`test_base`], REMOVED on drop (so a panicking
    /// assertion does not litter `/dev/shm`).
    struct TmpDir(PathBuf);

    impl TmpDir {
        fn new(tag: &str) -> Option<Self> {
            let base = test_base()?;
            let dir = base.join(format!("archimedes-sandbox-{tag}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            // `/dev/shm` is `1777` with a sticky bit and the dir we just made
            // is ours; nothing else to prepare.
            Some(Self(dir))
        }
        fn path(&self) -> &Path {
            &self.0
        }
        /// The dir as a shell-quotable string (the tags are uuid-derived, so
        /// no quoting characters can appear).
        fn display(&self) -> String {
            self.0.display().to_string()
        }
    }

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Pin `$HOME` to a fresh dir for the duration of a test, so
    /// `boundary::protected_dirs()` describes a synthetic layout instead of
    /// the developer's real `~/.agents` (the `boundary` tests' `pin_home`
    /// pattern). The env lock is held and the previous value is restored on
    /// drop — INCLUDING when an assertion panics.
    ///
    /// NOTE: a pinned `$HOME` lives under `/dev/shm`, which the sandbox
    /// READS (it sits under `/dev`), so a pinned home is only ever used for
    /// WRITE tests. The read test below uses the real `$HOME` instead.
    struct PinnedHome {
        _lock: std::sync::MutexGuard<'static, ()>,
        previous: Option<std::ffi::OsString>,
        /// The pinned `$HOME` itself (owned: removed when the pin ends).
        home: TmpDir,
    }

    impl PinnedHome {
        fn new(tag: &str) -> Option<Self> {
            let home = TmpDir::new(tag)?;
            let lock = crate::test_support::env_lock();
            // SAFETY: the env lock above is held for the whole pin, so no
            // other thread reads/writes `HOME` concurrently, and the
            // restore in `Drop` runs while it is still held.
            let previous = std::env::var_os("HOME");
            // SAFETY: as above.
            unsafe { std::env::set_var("HOME", home.path()) };
            Some(Self {
                _lock: lock,
                previous,
                home,
            })
        }
        fn path(&self) -> &Path {
            self.home.path()
        }
    }

    impl Drop for PinnedHome {
        fn drop(&mut self) {
            match self.previous.take() {
                // SAFETY: the env lock is still held (declared first, so it
                // drops last).
                Some(v) => unsafe { std::env::set_var("HOME", v) },
                // SAFETY: as above.
                None => unsafe { std::env::remove_var("HOME") },
            }
        }
    }

    /// The test ctx: `shell: Sandboxed` (the tier under test). HERMETIC —
    /// single-root boundary and an empty deny-list, NEVER the `$HOME`-
    /// reading boundary helpers (the `exec.rs` test-module rule).
    fn confined_ctx(cwd: &Path, reads: AccessPolicy) -> ToolCtx {
        ToolCtx {
            cwd: cwd.to_path_buf(),
            cancel: CancellationToken::new(),
            skill_roots: None,
            boundary: vec![cwd.to_path_buf()],
            file_policy: FilePolicy {
                reads,
                writes: AccessPolicy::Sandboxed,
                shell: AccessPolicy::Sandboxed,
            },
            protected: Vec::new(),
        }
    }

    /// Run `command` confined to `cwd`.
    async fn run(ctx: &ToolCtx, command: &str) -> ToolResult {
        execute_tool(ctx, "bash", &json!({ "command": command })).await
    }

    /// The first text block's text.
    fn text_of(r: &ToolResult) -> String {
        match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            other => panic!("expected a text block, got {other:?}"),
        }
    }

    // ── the constant table (the bug class this module existed to hide) ───

    /// The values as transcribed from the kernel UAPI header — the same
    /// numbers the module's `ACCESS_*` constants claim.
    const UAPI_EXPECTED: &[(&str, u64)] = &[
        ("LANDLOCK_ACCESS_FS_EXECUTE", 1 << 0),
        ("LANDLOCK_ACCESS_FS_WRITE_FILE", 1 << 1),
        ("LANDLOCK_ACCESS_FS_READ_FILE", 1 << 2),
        ("LANDLOCK_ACCESS_FS_READ_DIR", 1 << 3),
        ("LANDLOCK_ACCESS_FS_REMOVE_DIR", 1 << 4),
        ("LANDLOCK_ACCESS_FS_REMOVE_FILE", 1 << 5),
        ("LANDLOCK_ACCESS_FS_MAKE_CHAR", 1 << 6),
        ("LANDLOCK_ACCESS_FS_MAKE_DIR", 1 << 7),
        ("LANDLOCK_ACCESS_FS_MAKE_REG", 1 << 8),
        ("LANDLOCK_ACCESS_FS_MAKE_SOCK", 1 << 9),
        ("LANDLOCK_ACCESS_FS_MAKE_FIFO", 1 << 10),
        ("LANDLOCK_ACCESS_FS_MAKE_BLOCK", 1 << 11),
        ("LANDLOCK_ACCESS_FS_MAKE_SYM", 1 << 12),
        ("LANDLOCK_ACCESS_FS_REFER", 1 << 13),
        ("LANDLOCK_ACCESS_FS_TRUNCATE", 1 << 14),
        ("LANDLOCK_ACCESS_FS_IOCTL_DEV", 1 << 15),
        ("LANDLOCK_ACCESS_FS_RESOLVE_UNIX", 1 << 16),
    ];

    /// The constants this module actually uses, by the same names.
    const OURS: &[(&str, u64)] = &[
        ("LANDLOCK_ACCESS_FS_EXECUTE", ACCESS_EXECUTE),
        ("LANDLOCK_ACCESS_FS_WRITE_FILE", ACCESS_WRITE_FILE),
        ("LANDLOCK_ACCESS_FS_READ_FILE", ACCESS_READ_FILE),
        ("LANDLOCK_ACCESS_FS_READ_DIR", ACCESS_READ_DIR),
        ("LANDLOCK_ACCESS_FS_REMOVE_DIR", ACCESS_REMOVE_DIR),
        ("LANDLOCK_ACCESS_FS_REMOVE_FILE", ACCESS_REMOVE_FILE),
        ("LANDLOCK_ACCESS_FS_MAKE_CHAR", ACCESS_MAKE_CHAR),
        ("LANDLOCK_ACCESS_FS_MAKE_DIR", ACCESS_MAKE_DIR),
        ("LANDLOCK_ACCESS_FS_MAKE_REG", ACCESS_MAKE_REG),
        ("LANDLOCK_ACCESS_FS_MAKE_SOCK", ACCESS_MAKE_SOCK),
        ("LANDLOCK_ACCESS_FS_MAKE_FIFO", ACCESS_MAKE_FIFO),
        ("LANDLOCK_ACCESS_FS_MAKE_BLOCK", ACCESS_MAKE_BLOCK),
        ("LANDLOCK_ACCESS_FS_MAKE_SYM", ACCESS_MAKE_SYM),
        ("LANDLOCK_ACCESS_FS_REFER", ACCESS_REFER),
        ("LANDLOCK_ACCESS_FS_TRUNCATE", ACCESS_TRUNCATE),
        ("LANDLOCK_ACCESS_FS_IOCTL_DEV", ACCESS_IOCTL_DEV),
        ("LANDLOCK_ACCESS_FS_RESOLVE_UNIX", ACCESS_RESOLVE_UNIX),
    ];

    // ── the header audit: parser + coverage floor ────────────────────────

    /// Where the kernel UAPI header lives when the machine has one.
    const UAPI_HEADER_PATH: &str = "/usr/include/linux/landlock.h";

    /// Parse every `#define LANDLOCK_ACCESS_FS_*  (1ULL << N)` out of header
    /// text. Pure — no filesystem — so it can be driven with a synthetic
    /// v6.8 header. The name is matched as a whole token, so the NET rights
    /// (which reuse bit numbers 0..=3) cannot be mistaken for FS ones.
    fn parse_uapi_rights(header_text: &str) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        for line in header_text.lines() {
            let Some(rest) = line
                .trim_start()
                .strip_prefix("#define LANDLOCK_ACCESS_FS_")
            else {
                continue;
            };
            let Some(name_end) = rest.find(|c: char| c.is_whitespace()) else {
                continue;
            };
            let name = format!("LANDLOCK_ACCESS_FS_{}", &rest[..name_end]);
            let Some(bits) = rest[name_end..].split("<<").nth(1) else {
                continue;
            };
            let shift = bits
                .trim()
                .trim_end_matches(')')
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u32>().ok());
            if let Some(shift) = shift.filter(|s| *s < 64) {
                out.push((name, 1u64 << shift));
            }
        }
        out
    }

    /// What an audit of one header text produced.
    #[derive(Debug, Default)]
    struct HeaderAudit {
        /// Rights found in the header AND compared against our constants.
        checked: usize,
        /// Ours that the header does not define — NOT failures, simply not
        /// checkable on that header version (e.g. `IOCTL_DEV`, which a v6.8
        /// `linux-libc-dev` does not have).
        not_checkable: Vec<&'static str>,
        /// Everything that IS a failure: a bit mismatch, or an ABI-1 right
        /// missing from a header that defines Landlock at all.
        problems: Vec<String>,
    }

    /// The rights that have been in the UAPI header since Landlock landed
    /// (ABI 1, bits 0..=12): a header that defines `linux/landlock.h` at all
    /// MUST define these, so their absence is a broken header, not an old
    /// one. This is the coverage floor that keeps the audit from degenerating
    /// into checking nothing.
    const ABI1_FLOOR: &[&str] = &[
        "LANDLOCK_ACCESS_FS_EXECUTE",
        "LANDLOCK_ACCESS_FS_WRITE_FILE",
        "LANDLOCK_ACCESS_FS_READ_FILE",
        "LANDLOCK_ACCESS_FS_READ_DIR",
        "LANDLOCK_ACCESS_FS_REMOVE_DIR",
        "LANDLOCK_ACCESS_FS_REMOVE_FILE",
        "LANDLOCK_ACCESS_FS_MAKE_CHAR",
        "LANDLOCK_ACCESS_FS_MAKE_DIR",
        "LANDLOCK_ACCESS_FS_MAKE_REG",
        "LANDLOCK_ACCESS_FS_MAKE_SOCK",
        "LANDLOCK_ACCESS_FS_MAKE_FIFO",
        "LANDLOCK_ACCESS_FS_MAKE_BLOCK",
        "LANDLOCK_ACCESS_FS_MAKE_SYM",
    ];

    /// Compare our constants against parsed header rights, honouring the
    /// coverage floor. Returns the problems rather than panicking so the
    /// synthetic cases can assert on them.
    fn audit_uapi_rights(header_text: &str) -> HeaderAudit {
        let parsed = parse_uapi_rights(header_text);
        let mut audit = HeaderAudit::default();
        for (name, ours) in OURS {
            let Some((_, theirs)) = parsed.iter().find(|(n, _)| n == name) else {
                // Not defined: an older header. Only a failure if this right
                // has been in the header since Landlock landed.
                if ABI1_FLOOR.contains(name) {
                    audit.problems.push(format!(
                        "{name} is an ABI 1 right (it has been in the UAPI header \
                         since Landlock landed) but this header does not define it \
                         — the header is broken or truncated"
                    ));
                } else {
                    audit.not_checkable.push(name);
                }
                continue;
            };
            if ours != theirs {
                audit.problems.push(format!(
                    "{name}: our constant is {ours:#x} but the header says {theirs:#x} \
                     (bit {})",
                    theirs.trailing_zeros()
                ));
            }
            audit.checked += 1;
        }
        audit
    }

    /// Audit a header AND say out loud what could not be checked — a partial
    /// audit must never look like a full pass (this module's `SKIP-SANDBOX` /
    /// `SANDBOX-COVERAGE` convention).
    fn audit_rights_against_header(header_text: &str) -> HeaderAudit {
        let audit = audit_uapi_rights(header_text);
        if !audit.not_checkable.is_empty() {
            eprintln!(
                "SKIP-UAPI: {} right(s) absent from this kernel header so NOT checkable \
                 here: {} — {} right(s) WERE compared to the header",
                audit.not_checkable.len(),
                audit.not_checkable.join(", "),
                audit.checked
            );
        } else {
            eprintln!(
                "UAPI-COVERAGE: all {} right(s) compared to the header, 0 not checkable",
                audit.checked
            );
        }
        audit
    }

    /// A synthetic header body, in the exact shape the real one has
    /// (`#define NAME<TAB>(1ULL << N)`), including the NET rights — which
    /// share bit numbers with the FS ones and MUST NOT be matched.
    fn synthetic_header(defines: &[(&str, u32)]) -> String {
        let mut text = String::from(
            "/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */\n\
             #ifndef _LINUX_LANDLOCK_H\n#define _LINUX_LANDLOCK_H\n\n",
        );
        for (name, shift) in defines {
            text.push_str(&format!("#define {name}\t\t\t(1ULL << {shift})\n"));
        }
        text.push_str(
            "\n#define LANDLOCK_ACCESS_NET_BIND_TCP\t\t\t(1ULL << 0)\n\
             #define LANDLOCK_ACCESS_NET_CONNECT_TCP\t\t\t(1ULL << 1)\n\
             #endif /* _LINUX_LANDLOCK_H */\n",
        );
        text
    }

    /// The defines a v6.8 (Ubuntu 24.04 `linux-libc-dev`) header has: every
    /// right up to `TRUNCATE`, no `IOCTL_DEV` (v6.10) and no `RESOLVE_UNIX`.
    fn v6_8_defines() -> Vec<(&'static str, u32)> {
        OURS.iter()
            .filter(|(n, _)| {
                !matches!(
                    *n,
                    "LANDLOCK_ACCESS_FS_IOCTL_DEV" | "LANDLOCK_ACCESS_FS_RESOLVE_UNIX"
                )
            })
            .map(|(n, b)| (*n, b.trailing_zeros()))
            .collect()
    }

    /// (a) A v6.8-style header — the CI header — passes: the 15 rights it
    /// defines (the 13 ABI-1 ones plus REFER and TRUNCATE) are compared, and
    /// the 2 newer ones are reported as not checkable rather than failed.
    #[test]
    fn an_older_uapi_header_is_audited_for_what_it_defines() {
        let text = synthetic_header(&v6_8_defines());
        let audit = audit_rights_against_header(&text);
        assert!(
            audit.problems.is_empty(),
            "a v6.8 header must pass, got: {:?}",
            audit.problems
        );
        assert_eq!(
            audit.checked,
            OURS.len() - 2,
            "13 ABI-1 rights + REFER + TRUNCATE must be checked"
        );
        assert_eq!(
            audit.not_checkable,
            vec![
                "LANDLOCK_ACCESS_FS_IOCTL_DEV",
                "LANDLOCK_ACCESS_FS_RESOLVE_UNIX"
            ],
            "the two rights a v6.8 header lacks must be reported, not failed"
        );
    }

    /// (b) THE load-bearing case: the bug this guard exists for is a wrong
    /// BIT, and it must still be caught. `READ_FILE` declared as `1 << 0`
    /// is exactly the mistranscription that shipped once.
    #[test]
    fn a_mistranscribed_bit_in_the_header_fails_the_audit() {
        let mut defines = v6_8_defines();
        for (name, shift) in defines.iter_mut() {
            if *name == "LANDLOCK_ACCESS_FS_READ_FILE" {
                *shift = 0; // the original bug: READ_FILE transcribed as bit 0
            }
        }
        let audit = audit_rights_against_header(&synthetic_header(&defines));
        assert_eq!(audit.problems.len(), 1, "got: {:?}", audit.problems);
        let problem = &audit.problems[0];
        assert!(
            problem.contains("LANDLOCK_ACCESS_FS_READ_FILE"),
            "the failure must name the right, got: {problem}"
        );
        assert!(
            problem.contains("0x4") && problem.contains("0x1"),
            "the failure must show BOTH bits (ours 0x4, header 0x1), got: {problem}"
        );
    }

    /// (c) The coverage floor: a header that defines Landlock but not the
    /// rights that have existed since ABI 1 is a broken header, and an audit
    /// that quietly checks less must never go green.
    #[test]
    fn a_header_missing_an_abi_1_right_fails_the_coverage_floor() {
        let defines: Vec<(&str, u32)> = v6_8_defines()
            .into_iter()
            .filter(|(n, _)| *n != "LANDLOCK_ACCESS_FS_READ_DIR")
            .collect();
        let audit = audit_rights_against_header(&synthetic_header(&defines));
        assert!(
            audit
                .problems
                .iter()
                .any(|p| p.contains("LANDLOCK_ACCESS_FS_READ_DIR") && p.contains("ABI 1")),
            "a missing ABI-1 right must be a problem, got: {:?}",
            audit.problems
        );
    }

    /// (d) On a machine with a current header (this one, usually), the audit
    /// checks every right and skips nothing.
    #[test]
    fn the_installed_uapi_header_checks_every_right() {
        let Ok(text) = std::fs::read_to_string(UAPI_HEADER_PATH) else {
            eprintln!(
                "NOTE: no {UAPI_HEADER_PATH} on this machine — the header audit \
                 ran only on synthetic headers"
            );
            return;
        };
        let audit = audit_rights_against_header(&text);
        assert!(
            audit.problems.is_empty(),
            "our constants disagree with {UAPI_HEADER_PATH}: {:?}",
            audit.problems
        );
        assert_eq!(
            audit.checked,
            OURS.len(),
            "a current header must let every right be checked (not checkable: {:?})",
            audit.not_checkable
        );
        assert!(audit.not_checkable.is_empty());
    }

    /// THE regression test for the mistranscribed table: the hand-rolled
    /// `ACCESS_*` bits are the ABI, and a wrong one does not fail loudly —
    /// it enforces a different (weaker) right set while the ruleset still
    /// builds. When the UAPI header is on this machine, EVERY right it
    /// defines is parsed from it and compared; the table is ALSO checked
    /// against the transcribed copy above, so this test never skips.
    ///
    /// An OLD header checks FEWER rights, and that is not a failure: Linux
    /// v6.8 (Ubuntu 24.04's `linux-libc-dev`) has no `IOCTL_DEV` (added in
    /// v6.10) and no `RESOLVE_UNIX`, so those two are simply not checkable
    /// there — the test says so out loud (`SKIP-UAPI:`) instead of passing
    /// partially in silence. What keeps "fewer" honest is [`ABI1_FLOOR`]:
    /// every right present since ABI 1 MUST be found and compared, or the
    /// header is broken and the test fails.
    #[test]
    fn the_access_right_table_matches_the_kernel_uapi_header() {
        // (a) our constants vs. the transcribed UAPI values: always asserted.
        assert_eq!(
            OURS.len(),
            UAPI_EXPECTED.len(),
            "the two tables must list the same rights"
        );
        for (name, want) in UAPI_EXPECTED {
            let (_, got) = OURS
                .iter()
                .find(|(n, _)| n == name)
                .unwrap_or_else(|| panic!("{name} is missing from the module's constants"));
            assert_eq!(got, want, "{name}: the module's bit is not the UAPI value");
        }
        // (b) no duplicate bits, and the handled mask is exactly the union.
        for (i, (name_i, bit_i)) in OURS.iter().enumerate() {
            for (name_j, bit_j) in OURS.iter().skip(i + 1) {
                assert_eq!(bit_i & bit_j, 0, "{name_i} and {name_j} share a bit");
            }
        }
        let union = OURS.iter().fold(0u64, |m, (_, b)| m | b);
        assert_eq!(union, ALL_KNOWN_ACCESS, "ALL_KNOWN_ACCESS is not the union");

        // (c) the real header, when this machine has it: compare whatever it
        // defines, and never pretend the undefined ones were checked.
        let header = match std::fs::read_to_string(UAPI_HEADER_PATH) {
            Ok(text) => text,
            Err(_) => {
                eprintln!(
                    "NOTE: no {UAPI_HEADER_PATH} here — checked the transcribed \
                     table only"
                );
                return;
            }
        };
        let audit = audit_rights_against_header(&header);
        assert!(
            audit.problems.is_empty(),
            "our constants disagree with {UAPI_HEADER_PATH}: {:?}",
            audit.problems
        );
    }

    /// The ABI mask: a bit beyond the kernel's ABI fails ruleset creation
    /// `EAGAIN`, so the handled set must be derived from the probe. These
    /// floors are the UAPI's own (REFER 2, TRUNCATE 3, IOCTL_DEV 5,
    /// RESOLVE_UNIX 9).
    #[test]
    fn the_handled_mask_follows_the_probed_abi() {
        assert_eq!(
            handled_access_for(1),
            (1u64 << 13) - 1,
            "ABI 1 must handle exactly bits 0..=12"
        );
        assert_eq!(handled_access_for(2) & ACCESS_REFER, ACCESS_REFER);
        assert_eq!(handled_access_for(2) & ACCESS_TRUNCATE, 0);
        assert_eq!(handled_access_for(3) & ACCESS_TRUNCATE, ACCESS_TRUNCATE);
        assert_eq!(handled_access_for(4) & ACCESS_IOCTL_DEV, 0);
        assert_eq!(handled_access_for(5) & ACCESS_IOCTL_DEV, ACCESS_IOCTL_DEV);
        // `RESOLVE_UNIX` is never handled (module doc), even on a kernel
        // that supports it.
        assert_eq!(handled_access_for(99) & ACCESS_RESOLVE_UNIX, 0);
        assert_eq!(
            handled_access_for(99),
            ALL_KNOWN_ACCESS & !ACCESS_RESOLVE_UNIX
        );
        // And the mask must be accepted by THIS kernel (the probe and the
        // mask cannot disagree, or every confined run fails `EAGAIN`).
        let abi = ruleset_abi_version();
        if abi > 0 {
            let mask = handled_access_for(abi as u32);
            let attr = LandlockRulesetAttr {
                handled_access_fs: mask,
            };
            // SAFETY: `attr` is a live attr of the given size.
            let fd =
                unsafe { create_ruleset(&attr, std::mem::size_of::<LandlockRulesetAttr>(), 0) };
            assert!(
                fd >= 0,
                "the ABI-masked handle set was rejected ({}): the mask is out of sync with the kernel",
                std::io::Error::last_os_error()
            );
            // SAFETY: `fd` is the ruleset fd we just created.
            unsafe { libc::close(fd) };
        }
    }

    // ── the device nodes ─────────────────────────────────────────────────

    /// The devices that are granted READ + WRITE (+ `IOCTL_DEV` where the fd
    /// is a device) are ONLY the ones an ordinary command redirects to.
    /// `/dev/tty` is NOT one of them, and must never be added back: a
    /// confined child shares the Worker's controlling terminal (`exec.rs`
    /// spawns with `process_group(0)`, which does not detach the ctty, and
    /// pipes only stdout/stderr — stdin is inherited), so `WRITE_FILE` on
    /// `/dev/tty` is a direct write channel to the USER's terminal (escape
    /// sequences, screen/mouse-state manipulation) and, on kernels < 6.2,
    /// `TIOCSTI` keystroke INJECTION via `IOCTL_DEV`. The grant buys nothing:
    /// the child's own stdio is piped, so it never opens `/dev/tty` to
    /// function. A READ grant is equally unnecessary — `/dev` is already
    /// ruled read+execute.
    #[test]
    fn the_write_granted_devices_exclude_the_controlling_terminal() {
        assert!(
            !READ_WRITE_DEVICES.contains(&"/dev/tty"),
            "/dev/tty must not be in the read-write device list: it is the user's \
             terminal, and WRITE_FILE|IOCTL_DEV on it lets a sandboxed command write \
             escape sequences to it or inject keystrokes (TIOCSTI, kernels < 6.2). \
             List is {READ_WRITE_DEVICES:?}"
        );
        // The nodes ordinary commands genuinely need write access to (a shell
        // redirects to `/dev/null`; `git --version` fails outright without it)
        // must stay granted, or this "fix" is just a broken list.
        for node in [
            "/dev/null",
            "/dev/zero",
            "/dev/full",
            "/dev/random",
            "/dev/urandom",
        ] {
            assert!(
                READ_WRITE_DEVICES.contains(&node),
                "{node} must stay read-write granted — ordinary commands need it"
            );
        }
    }

    // ── availability ─────────────────────────────────────────────────────

    /// The probe is the kernel's answer, and it must agree with whether a
    /// ruleset can actually be built: Settings greys the option out from
    /// exactly this signal, so a probe that lies in EITHER direction is a
    /// bug (offering a tier that would fail, or hiding one that works).
    #[test]
    fn the_availability_probe_reflects_the_syscall_result() {
        let probe = landlock_available();
        let built = Sandbox::create(Path::new("/")).is_ok();
        assert_eq!(probe, built, "the probe and a ruleset build disagree");
    }

    /// The always-visible verdict line: either the behavioral tests below
    /// ran on this kernel, or the run contains `SKIP-SANDBOX` markers.
    /// Green + skips means UNVERIFIED, and this is how you tell from the
    /// output without reading the code.
    #[test]
    fn sandbox_coverage_is_reported() {
        let skips = SKIPPED.load(Ordering::SeqCst);
        if landlock_available() && skips == 0 {
            println!(
                "SANDBOX-COVERAGE: ENFORCED — Landlock ABI {}, 0 behavioral tests skipped",
                ruleset_abi_version()
            );
        } else if !landlock_available() {
            println!(
                "SANDBOX-COVERAGE: NONE — this kernel has no Landlock, the confinement \
                 tests above SKIPPED (see SKIP-SANDBOX markers); only the fail-closed \
                 path was verified"
            );
        } else {
            println!(
                "SANDBOX-COVERAGE: PARTIAL — {skips} behavioral test(s) SKIPPED \
                 (SKIP-SANDBOX markers above); the rest were enforced on ABI {}",
                ruleset_abi_version()
            );
        }
    }

    // ── confinement (behavioral; skipped on a kernel without Landlock) ───

    /// THE point of the tier: a confined command cannot WRITE outside the
    /// write boundary. The target is a dir the test process OWNS (a
    /// root-owned path would pass vacuously — `EACCES` there would prove
    /// nothing about the sandbox).
    #[tokio::test]
    async fn a_confined_command_cannot_write_outside_the_boundary() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("out-write").expect("base exists");
        let outside = TmpDir::new("out-target").expect("base exists");
        let target = format!("{}/payload.txt", outside.display());
        let r = run(
            &confined_ctx(cwd.path(), AccessPolicy::Allow),
            &format!("echo pwn > {target}"),
        )
        .await;
        assert!(r.is_error, "the write must fail: {:?}", text_of(&r));
        assert!(
            !outside.0.join("payload.txt").exists(),
            "the payload must not have landed outside the boundary"
        );
        let _ = base;
    }

    /// READS are confined too — this is the half the mistranscribed table
    /// silently left UNENFORCED (`READ_FILE` was never in the handled mask,
    /// so every read outside the allow-list worked: `cat /etc/passwd`,
    /// `cat ~/.bashrc`). Target: a file in a dir the test process owns.
    #[tokio::test]
    async fn a_confined_command_cannot_read_outside_the_allow_list() {
        let base = require_sandbox!();
        let outside = match private_dir_outside_the_read_allow_list() {
            Some(dir) => dir,
            None => {
                eprintln!("SKIP-SANDBOX: no test-owned dir outside the read allow-list");
                SKIPPED.fetch_add(1, Ordering::SeqCst);
                return;
            }
        };
        let cwd = TmpDir::new("out-read").expect("base exists");
        let secret_name = format!("archimedes-sandbox-secret-{}", uuid::Uuid::new_v4());
        let secret_path = outside.join(&secret_name);
        std::fs::write(&secret_path, "DO-NOT-READ").unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let _cleanup = Cleanup(secret_path.clone());
        let secret = secret_path.display().to_string();
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        // (a) the payload must not come back…
        let r = run(&ctx, &format!("cat {secret}")).await;
        assert!(r.is_error, "the read must fail: {:?}", text_of(&r));
        assert!(
            !text_of(&r).contains("DO-NOT-READ"),
            "the confined child leaked the file content: {:?}",
            text_of(&r)
        );
        // (b) …and the CONTRAST, so this cannot pass vacuously: the same
        // `cat` with the sandbox OFF reads it.
        let loose = ToolCtx {
            file_policy: FilePolicy {
                reads: AccessPolicy::Allow,
                writes: AccessPolicy::Allow,
                shell: AccessPolicy::Allow,
            },
            ..confined_ctx(cwd.path(), AccessPolicy::Allow)
        };
        let r = run(&loose, &format!("cat {secret}")).await;
        assert!(
            !r.is_error && text_of(&r).contains("DO-NOT-READ"),
            "the file must be readable when the sandbox is off: {:?}",
            text_of(&r)
        );
        // (c) and the very same confined child still reads its OWN file, so
        // the denial above is about location, not about `cat` being broken.
        std::fs::write(cwd.path().join("mine.txt"), "MINE").unwrap();
        let r = run(&ctx, "cat mine.txt").await;
        assert!(
            !r.is_error && text_of(&r).contains("MINE"),
            "a read inside the boundary still works: {:?}",
            text_of(&r)
        );
        let _ = base;
    }

    /// `TRUNCATE` (ABI 3+) must be HANDLED and granted only inside the
    /// boundary. Unhandled, `truncate(2)` by path is a write primitive that
    /// works anywhere DAC allows — including emptying a file outside the
    /// session. Inside the boundary it must still work (`echo x > f`
    /// truncates, so a missing grant breaks ordinary shell use).
    #[tokio::test]
    async fn a_confined_command_cannot_truncate_outside_the_boundary() {
        let base = require_sandbox!();
        if ruleset_abi_version() < 3 {
            eprintln!(
                "SKIP-SANDBOX: truncate needs Landlock ABI 3 (this kernel is {})",
                ruleset_abi_version()
            );
            SKIPPED.fetch_add(1, Ordering::SeqCst);
            return;
        }
        let cwd = TmpDir::new("out-trunc").expect("base exists");
        // The enforcement-relevant half, asserted directly: on a kernel that
        // knows `TRUNCATE`, the ruleset must HANDLE it (an unhandled right is
        // simply not enforced) and must GRANT it inside the boundary (without
        // the grant, `echo x > existing_file` — which truncates — breaks).
        // The behavioral probe below cannot isolate this: a `truncate(2)`
        // outside the boundary is already refused by the missing
        // `WRITE_FILE`, so it stays green either way.
        let handled = handled_access_for(ruleset_abi_version() as u32);
        assert_ne!(
            handled & ACCESS_TRUNCATE,
            0,
            "ABI {} knows TRUNCATE, so the ruleset must handle it",
            ruleset_abi_version()
        );
        // (A missing GRANT is caught behaviorally below: `echo new > f.txt`
        // on an existing file needs `O_TRUNC`.)
        let outside = TmpDir::new("trunc-target").expect("base exists");
        std::fs::write(outside.path().join("data.txt"), "KEEP-ME").unwrap();
        let target = format!("{}/data.txt", outside.display());
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        for cmd in [format!("truncate -s 0 {target}"), format!(": > {target}")] {
            let r = run(&ctx, &cmd).await;
            assert!(r.is_error, "{cmd} must fail: {:?}", text_of(&r));
            assert_eq!(
                std::fs::read_to_string(outside.path().join("data.txt")).unwrap(),
                "KEEP-ME",
                "{cmd} truncated a file outside the boundary"
            );
        }
        // Inside the boundary, truncating an existing file is ordinary use.
        std::fs::write(cwd.path().join("f.txt"), "OLD-CONTENT").unwrap();
        let r = run(&ctx, "echo new > f.txt && cat f.txt").await;
        assert!(
            !r.is_error && text_of(&r).contains("new"),
            "overwriting an existing file inside the boundary must work: {:?}",
            text_of(&r)
        );
        let _ = base;
    }

    /// `MAKE_SYM` must be HANDLED: unhandled, a confined child plants a
    /// symlink ANYWHERE DAC allows (`ln -s /etc/passwd ~/.somefile`), which
    /// the later unsandboxed `read`/`write` tools then follow out of the
    /// boundary.
    #[tokio::test]
    async fn a_confined_command_cannot_plant_a_symlink_outside_the_boundary() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("out-ln").expect("base exists");
        let outside = TmpDir::new("ln-target").expect("base exists");
        let link = format!("{}/planted", outside.display());
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        let r = run(&ctx, &format!("ln -s /etc/passwd {link}")).await;
        assert!(r.is_error, "the symlink must fail: {:?}", text_of(&r));
        assert!(
            !outside.0.join("planted").exists(),
            "a symlink was planted outside the boundary"
        );
        // And a hardlink (MAKE_REG + REFER) is equally refused.
        std::fs::write(outside.path().join("victim"), "v").unwrap();
        let r = run(&ctx, &format!("ln victim {}/hard", outside.display())).await;
        assert!(
            r.is_error && !outside.0.join("hard").exists(),
            "a hardlink was created outside the boundary: {:?}",
            text_of(&r)
        );
        // CONTRAST: the same `ln -s` inside the boundary works (so the deny
        // above is about the location, not about `ln` missing).
        let r = run(&ctx, "ln -s /etc/passwd mylink && ls -l mylink").await;
        assert!(
            !r.is_error && text_of(&r).contains("mylink"),
            "symlinks inside the boundary must work: TEXT={:?} DETAILS={:?} EXIT={:?}",
            text_of(&r),
            r.details,
            r.content.len()
        );
        let _ = base;
    }

    /// Whether THIS process can open its controlling terminal for writing.
    /// The confined child INHERITS the Worker's controlling terminal (see
    /// [`the_write_granted_devices_exclude_the_controlling_terminal`]), so
    /// this probe is the confined child's reachability too — and, like the
    /// other helpers, it asks what the UNCONFINED process can do, so a later
    /// denial proves Landlock did it and not DAC. `O_NOCTTY`: probing must
    /// never make the test process acquire a terminal it did not have. Under
    /// `cargo test` with no ctty the open fails `ENXIO` and the behavioral
    /// test below skips loudly.
    fn controlling_terminal_is_writeable() -> bool {
        let path = CString::new("/dev/tty").unwrap();
        // SAFETY: `path` is a live NUL-terminated path; `open` only resolves it.
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_NOCTTY) };
        if fd < 0 {
            return false;
        }
        // SAFETY: `fd` is the fd we just opened.
        unsafe { libc::close(fd) };
        true
    }

    /// THE tty hole: `echo x > /dev/tty` from a confined command must NOT
    /// reach the terminal. Without the fix the confined child inherits the
    /// Worker's ctty, `/dev/tty` is write-granted, and the redirect succeeds
    /// — a sandboxed command writing raw escape sequences into the user's
    /// terminal (and `ioctl(TIOCSTI)` keystroke injection on kernels < 6.2).
    /// The CONTRAST half (an unconfined run of the same command) proves the
    /// refusal is the sandbox and not a machine without a usable tty.
    #[tokio::test]
    async fn a_confined_command_cannot_write_to_the_controlling_terminal() {
        let base = require_sandbox!();
        if !controlling_terminal_is_writeable() {
            eprintln!(
                "SKIP-SANDBOX: no controlling terminal writable here (`/dev/tty` is \
                 not openable) — skip #{}",
                SKIPPED.fetch_add(1, Ordering::SeqCst) + 1
            );
            return;
        }
        let cwd = TmpDir::new("tty-write").expect("base exists");
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        // Plain text only: the command must fail, so nothing is written to
        // the real terminal either way (and a stray byte is harmless).
        let r = run(&ctx, "echo x > /dev/tty").await;
        assert!(
            r.is_error,
            "a confined command wrote to the controlling terminal: {:?}",
            text_of(&r)
        );
        // CONTRAST: the same command unconfined succeeds, so the refusal
        // above is the ruleset (the probe already proved DAC allows it).
        let loose = ToolCtx {
            file_policy: FilePolicy {
                reads: AccessPolicy::Allow,
                writes: AccessPolicy::Allow,
                shell: AccessPolicy::Allow,
            },
            ..confined_ctx(cwd.path(), AccessPolicy::Allow)
        };
        let r = run(&loose, "echo x > /dev/tty").await;
        assert!(
            !r.is_error,
            "the unconfined run must be able to open /dev/tty (the tty probe lies): {:?}",
            text_of(&r)
        );
        // And the confined child is not crippled: `/dev/null` — a node it
        // genuinely needs — still works.
        let r = run(&ctx, "echo ok > /dev/null && echo ALL_OK").await;
        assert!(
            !r.is_error && text_of(&r).contains("ALL_OK"),
            "/dev/null must stay writable for a confined command: {:?}",
            text_of(&r)
        );
        let _ = base;
    }

    /// CONTRAST (so the write test above cannot pass vacuously): the SAME
    /// command with `shell: Allow` writes the file.
    #[tokio::test]
    async fn the_same_command_writes_freely_when_the_sandbox_is_off() {
        let Some(base) = test_base() else {
            eprintln!("SKIP-SANDBOX: no /dev/shm on this machine");
            return;
        };
        let cwd = TmpDir::new("free-write").expect("base exists");
        let outside = TmpDir::new("free-target").expect("base exists");
        let ctx = ToolCtx {
            file_policy: FilePolicy {
                reads: AccessPolicy::Allow,
                writes: AccessPolicy::Allow,
                shell: AccessPolicy::Allow,
            },
            ..confined_ctx(cwd.path(), AccessPolicy::Allow)
        };
        let r = run(
            &ctx,
            &format!("echo pwn > {}/payload.txt", outside.display()),
        )
        .await;
        assert!(
            !r.is_error,
            "an unconfined run writes anywhere: {:?}",
            text_of(&r)
        );
        assert!(outside.0.join("payload.txt").exists());
        let _ = base;
    }

    /// Inside the boundary a confined command works NORMALLY — create,
    /// overwrite, rename, unlink, and temp files.
    #[tokio::test]
    async fn a_confined_command_works_normally_inside_the_boundary() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("in-write").expect("base exists");
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        let r = run(
            &ctx,
            "echo hi > inside.txt && cat inside.txt && mkdir -p sub && \
             mv inside.txt sub/moved.txt && rm -f sub/gone.txt; \
             TMPDIR=/tmp mktemp >/dev/null && echo ALL_OK",
        )
        .await;
        assert!(
            !r.is_error,
            "inside the boundary it runs: {:?}",
            text_of(&r)
        );
        assert!(text_of(&r).contains("hi"));
        assert!(text_of(&r).contains("ALL_OK"));
        assert!(cwd.path().join("sub/moved.txt").exists());
        let _ = base;
    }

    /// The dynamic linker and the system reads still work: the ruleset
    /// exists to confine the CHILD, and a shell that cannot `ld.so` or read
    /// `/usr` is not a shell. The doc file is discovered at run time
    /// (package names are distro-dependent).
    #[tokio::test]
    async fn a_confined_command_still_reads_the_system_dirs() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("sys-read").expect("base exists");
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        // The doc file is discovered INSIDE the sandbox (package names are
        // distro-dependent), and the host is checked for the same glob so
        // the test cannot pass vacuously on the "no docs here" branch.
        let docs_exist = std::fs::read_dir("/usr/share/doc")
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| e.path().join("copyright").is_file())
            })
            .unwrap_or(false);
        let r = run(
            &ctx,
            "f=$(ls /usr/share/doc/*/copyright 2>/dev/null | head -1); \
             test -n \"$f\" || { echo NO_DOC_ON_THIS_SYSTEM; exit 0; }; \
             head -c 40 \"$f\" >/dev/null && echo DOC_READ_OK",
        )
        .await;
        assert!(!r.is_error, "system reads must work: {:?}", text_of(&r));
        if docs_exist {
            assert!(
                text_of(&r).contains("DOC_READ_OK"),
                "a /usr/share/doc/*/copyright exists on this host, so the confined read must succeed: {:?}",
                text_of(&r)
            );
        } else {
            assert!(text_of(&r).contains("NO_DOC_ON_THIS_SYSTEM"));
        }
        // And the shell itself still runs (the loader path + builtins).
        let r = run(&ctx, "echo hi").await;
        assert_eq!(text_of(&r), "hi\n");
        let r = run(
            &ctx,
            "for i in 1 2; do printf '%s' \"$i\"; done; echo; true",
        )
        .await;
        assert!(!r.is_error, "builtins work: {:?}", text_of(&r));
        let _ = base;
    }

    /// A confined shell is still a SHELL: `/dev/null` read-write, a name
    /// lookup, a TLS fetch, `git`, and temp files. Every one of these broke
    /// the moment reads became genuinely confined (they need `/etc` files
    /// the ruleset now grants explicitly), so this is the usability
    /// regression test for the grants above.
    #[tokio::test]
    async fn a_confined_shell_can_still_do_ordinary_things() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("usable").expect("base exists");
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        // `/dev/null` + redirection + a temp file.
        let r = run(
            &ctx,
            "echo hi > ./f && test $(cat ./f) = hi && echo REDIRECT_OK; \
             echo to-null > /dev/null && echo DEVNULL_OK; \
             TMPDIR=/tmp mktemp >/dev/null && echo MKTEMP_OK",
        )
        .await;
        assert!(
            text_of(&r).contains("REDIRECT_OK")
                && text_of(&r).contains("DEVNULL_OK")
                && text_of(&r).contains("MKTEMP_OK"),
            "ordinary redirection must work: {:?}",
            text_of(&r)
        );
        // The resolver (`/etc/resolv.conf` + `nsswitch.conf` + `hosts`) and
        // the identity files (`passwd`): `git` and `whoami` are useless
        // without them.
        let r = run(
            &ctx,
            "getent hosts localhost > /dev/null && echo LOOKUP_OK; whoami | grep -q . && echo ID_OK",
        )
        .await;
        assert!(
            text_of(&r).contains("LOOKUP_OK") && text_of(&r).contains("ID_OK"),
            "name lookup and identity must work: {:?}",
            text_of(&r)
        );
        // `git` (the CA bundle is for the TLS check below, git's config for
        // this one: git EXITS 128 when `$HOME/.gitconfig` exists and cannot
        // be read, so a confined shell could not use its own repo).
        let git_ok = std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        let r = run(&ctx, "git --version > /dev/null && echo GIT_OK").await;
        if git_ok {
            assert!(
                text_of(&r).contains("GIT_OK"),
                "git must run under confinement: {:?}",
                text_of(&r)
            );
        }
        // TLS: needs the CA trust anchors. Skips (loudly) with no network.
        let online = std::process::Command::new("curl")
            .args(["-sS", "-m", "5", "-o", "/dev/null", "https://example.com"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if online {
            let r = run(
                &ctx,
                "curl -sS -m 10 -o /dev/null -w '%{http_code}' https://example.com",
            )
            .await;
            assert!(
                !r.is_error && text_of(&r).starts_with("200"),
                "a confined TLS fetch must work (the CA bundle must be readable): {:?}",
                text_of(&r)
            );
        } else {
            eprintln!("SKIP-SANDBOX: no network for the confined TLS fetch");
            SKIPPED.fetch_add(1, Ordering::SeqCst);
        }
        let _ = base;
    }

    /// `/proc` is granted READ + EXECUTE — and the grant is LOAD-BEARING:
    /// without a `/proc` rule a confined `ls /proc` is refused (`EACCES`),
    /// which is exactly the kind of ordinary command the tier must not
    /// break. `/proc` is deliberately given NO write rights: a writable
    /// `/proc/self/…` is the classic escape hatch (`oom_score_adj`, the
    /// `core_pattern`-adjacent paths).
    ///
    /// The no-write half is asserted BEHAVIORALLY: writing the task's own
    /// `oom_score_adj` is permitted by DAC (it is the caller's own file), so
    /// the ONLY thing that can refuse it is the ruleset. With the previous
    /// mistranscribed table the `READ_EXECUTE` mask carried the kernel's
    /// real `WRITE_FILE` bit and this write SUCCEEDED (measured); the old
    /// test asserted `READ_EXECUTE & FORBIDDEN == 0` — constants against
    /// constants — and passed anyway.
    #[tokio::test]
    async fn proc_is_read_execute_only_and_stays_listable() {
        // The structural half: the mask `/proc` gets carries NO write bit.
        const FORBIDDEN: u64 = ACCESS_WRITE_FILE
            | ACCESS_REMOVE_FILE
            | ACCESS_MAKE_CHAR
            | ACCESS_MAKE_BLOCK
            | ACCESS_MAKE_REG
            | ACCESS_MAKE_FIFO
            | ACCESS_MAKE_SOCK
            | ACCESS_MAKE_SYM;
        assert_eq!(
            READ_EXECUTE & FORBIDDEN,
            0,
            "the mask used for /proc must carry no write right"
        );
        let base = require_sandbox!();
        let cwd = TmpDir::new("proc").expect("base exists");
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        let r = run(
            &ctx,
            "ls /proc > /dev/null && cat /proc/self/status > /dev/null && echo PROC_OK",
        )
        .await;
        assert!(
            !r.is_error && text_of(&r).contains("PROC_OK"),
            "/proc must stay listable+readable under confinement: {:?}",
            text_of(&r)
        );
        // The behavioral half: the task's OWN tunable, writable by DAC, must
        // be refused by the ruleset.
        let writable_by_dac = std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/self/oom_score_adj")
            .is_ok();
        let r = run(&ctx, "echo 100 > /proc/self/oom_score_adj").await;
        if writable_by_dac {
            assert!(
                r.is_error,
                "/proc/self/oom_score_adj is DAC-writable, so only the ruleset \
                 can refuse it — and it did NOT: {:?}",
                text_of(&r)
            );
        } else {
            eprintln!("SKIP-SANDBOX: /proc/self/oom_score_adj is not DAC-writable here");
            SKIPPED.fetch_add(1, Ordering::SeqCst);
        }
        let _ = base;
    }

    /// REAP UNDER CONFINEMENT: a backgrounded grandchild must die with the
    /// process group on cancel. Registering a `pre_exec` closure forces
    /// std's fork+exec path (it refuses `posix_spawn` with closures), so
    /// `process_group(0)` must still make the child a group leader or the
    /// negative-pid kill misses the grandchild — and the existing bash
    /// tests never activate the sandbox, so they prove nothing here.
    ///
    /// The survivor check is scoped to THIS run's own process GROUP (the
    /// child reports its `pgid`, which every grandchild inherits): a
    /// machine-wide count of `sleep 1000` reddens whenever anything else on
    /// the box happens to sleep for 1000 seconds, and no argv marker works
    /// here — `sleep 1000 # marker &` puts the `&` inside the comment (so
    /// nothing is backgrounded and the test would pass vacuously) and
    /// `sleep 1000 marker` is an invalid interval.
    #[tokio::test]
    async fn a_confined_backgrounded_grandchild_is_reaped_with_the_group() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("reap").expect("base exists");
        let token = CancellationToken::new();
        let ctx = ToolCtx {
            cancel: token.clone(),
            ..confined_ctx(cwd.path(), AccessPolicy::Allow)
        };
        // The child prints its own pgid — equal to its own pid only if
        // `process_group(0)` made it a GROUP LEADER, which is the exact
        // property under test — then backgrounds a grandchild that nothing
        // but the group kill can reach.
        let cmd = "sleep 1000 & ps -o pgid= -p $$ | tr -d ' ' | sed 's/^/PGID=/' && echo started";
        let handle =
            tokio::spawn(
                async move { execute_tool(&ctx, "bash", &json!({ "command": cmd })).await },
            );
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        // Sample the group BEFORE the cancel: if the grandchild never joined
        // the group, the "no survivors" check below would pass vacuously.
        let before = group_snapshot();
        token.cancel();
        let r = tokio::time::timeout(std::time::Duration::from_secs(15), handle)
            .await
            .expect("the call returns (no unbounded wait for the pipes' EOF)")
            .unwrap();
        assert_eq!(r.details.as_ref().unwrap()["cancelled"], true);
        assert!(text_of(&r).contains("started"), "output is still captured");
        let pgid: i64 = text_of(&r)
            .lines()
            .find_map(|l| l.strip_prefix("PGID="))
            .and_then(|s| s.trim().parse().ok())
            .expect("the child reported its own pgid");
        let members = group_members(&before, pgid);
        assert!(
            members.len() >= 2,
            "the backgrounded grandchild never shared the child's process group {pgid}: {members:?}"
        );
        let survivors = group_members(&group_snapshot(), pgid);
        assert!(
            survivors.is_empty(),
            "processes in the confined child's group {pgid} escaped the group kill: {survivors:?}"
        );
        let _ = base;
    }

    /// `ps` with the process GROUP first (for the scoped reap check above).
    fn group_snapshot() -> String {
        let out = std::process::Command::new("ps")
            .args(["-eo", "pgid,pid,args"])
            .output()
            .expect("ps");
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    /// The lines of a `ps -eo pgid,pid,args` snapshot in process group `pgid`.
    fn group_members(snapshot: &str, pgid: i64) -> Vec<String> {
        snapshot
            .lines()
            .filter(|l| {
                l.split_whitespace()
                    .next()
                    .and_then(|g| g.parse::<i64>().ok())
                    == Some(pgid)
            })
            .map(|l| l.to_string())
            .collect()
    }

    /// The confinement is CHILD-ONLY: the same session's `read` tool still
    /// reads beyond the boundary (`reads: Allow`), and the test process
    /// itself still writes there. If the ruleset ever leaked into the
    /// Worker, every later tool call — and the desktop itself — would be
    /// confined.
    #[tokio::test]
    async fn the_sandbox_confines_the_child_and_never_the_worker() {
        let base = require_sandbox!();
        let cwd = TmpDir::new("child-only").expect("base exists");
        let outside = TmpDir::new("child-only-target").expect("base exists");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE").unwrap();
        let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
        // First a confined run (it installs the ruleset in ITS child only)…
        let r = run(&ctx, "echo confined").await;
        assert!(!r.is_error, "the confined run works: {:?}", text_of(&r));
        // …then a NON-bash tool in the same ctx reads beyond the boundary.
        let r = execute_tool(
            &ctx,
            "read",
            &json!({ "path": format!("{}/secret.txt", outside.display()) }),
        )
        .await;
        assert!(
            !r.is_error,
            "the Worker must not be confined: {:?}",
            text_of(&r)
        );
        assert!(text_of(&r).contains("OUTSIDE"));
        // And the Worker process itself is unconfined.
        std::fs::write(outside.path().join("worker-wrote.txt"), "ok")
            .expect("the Worker is unconfined");
        let _ = base;
    }

    // ── the protected dirs ───────────────────────────────────────────────

    /// THE exploit the write tools' deny-list exists to stop, at the
    /// TIGHTEST tier: a session whose `cwd` IS a protected agent-definition
    /// dir must NOT be able to rewrite a `SKILL.md` there. The old code
    /// ruled `write_roots(cwd)` read-write unconditionally, so the deny-list
    /// was reachable around through `bash`.
    #[tokio::test]
    async fn a_session_cwd_inside_a_protected_dir_cannot_write_there() {
        let base = require_sandbox!();
        // A PROTECTED dir, laid out exactly like the real one, under a
        // pinned `$HOME` (the `boundary` tests' `pin_home` pattern — never
        // the developer's real `~/.agents`). The pinned home is itself under
        // `/dev/shm`, so the test never touches a root-owned path.
        let pin = match PinnedHome::new("protected-home") {
            Some(pin) => pin,
            None => panic!("no /dev/shm"),
        };
        let skills = pin.path().join(".agents/skills/demo");
        std::fs::create_dir_all(&skills).unwrap();
        std::fs::write(skills.join("SKILL.md"), "original instructions").unwrap();
        // The protected set must actually contain the dir under test, or the
        // test proves nothing.
        assert!(
            crate::agent::boundary::protected_dirs()
                .iter()
                .any(|p| skills.starts_with(p)),
            "the pinned `$HOME` must produce a protected dir containing the skill"
        );
        // The session is rooted IN the protected dir — the tightest tier, and
        // the case the write tools' deny-list exists for.
        let ctx = confined_ctx(&skills, AccessPolicy::Allow);
        let r = run(&ctx, "echo PWNED > SKILL.md").await;
        assert!(
            r.is_error,
            "a confined write to a protected SKILL.md must fail: {:?}",
            text_of(&r)
        );
        assert_eq!(
            std::fs::read_to_string(skills.join("SKILL.md")).unwrap(),
            "original instructions",
            "the confined shell rewrote its own instructions (self-modification)"
        );
        // And the SUBTREE is protected, not just the leaf: creating a file in
        // a sibling dir of the protected tree is refused too.
        let sibling = pin.path().join(".agents/skills/other");
        std::fs::create_dir_all(&sibling).unwrap();
        let r = run(
            &ctx,
            &format!("echo PWNED > {}", sibling.join("x.txt").display()),
        )
        .await;
        assert!(
            r.is_error,
            "a protected subtree must not be writable: {:?}",
            text_of(&r)
        );
        // CONTRAST: the same `echo >` into a NON-protected session dir works,
        // so the refusals above are the protection, not a broken write path.
        let free = TmpDir::new("protected-contrast").expect("base exists");
        let r = run(
            &confined_ctx(free.path(), AccessPolicy::Allow),
            "echo ok > f.txt",
        )
        .await;
        assert!(
            !r.is_error && free.path().join("f.txt").exists(),
            "an ordinary session dir must be writable: {:?}",
            text_of(&r)
        );
        let _ = base;
    }

    /// A write root that merely CONTAINS a protected dir: the protected
    /// subtree stays read-only while the rest of the tree keeps working
    /// (Landlock cannot subtract, so the ancestors of the protected dir are
    /// ruled read+execute and their siblings read-write — see
    /// [`Sandbox::rule_write_root`]).
    #[tokio::test]
    async fn a_write_root_containing_a_protected_dir_stays_writable_around_it() {
        let base = require_sandbox!();
        let pin = match PinnedHome::new("carve-home") {
            Some(pin) => pin,
            None => panic!("no /dev/shm"),
        };
        let skills = pin.path().join(".agents/skills");
        std::fs::create_dir_all(&skills).unwrap();
        std::fs::write(skills.join("SKILL.md"), "original").unwrap();
        // The session is rooted at `$HOME` itself (a real setup: a session
        // opened on the home directory).
        std::fs::create_dir_all(pin.path().join("project")).unwrap();
        assert!(!crate::agent::boundary::protected_dirs().is_empty());
        let ctx = confined_ctx(pin.path(), AccessPolicy::Allow);
        // The protected file survives every write primitive.
        let skill = skills.join("SKILL.md");
        for cmd in [
            format!("echo PWNED > {}", skill.display()),
            format!("truncate -s 0 {}", skill.display()),
            format!("rm -f {}", skill.display()),
            format!("ln -s /etc/passwd {}", skills.join("planted").display()),
        ] {
            let r = run(&ctx, &cmd).await;
            assert!(r.is_error, "{cmd} must fail: {:?}", text_of(&r));
        }
        assert_eq!(
            std::fs::read_to_string(&skill).unwrap(),
            "original",
            "a protected file was modified through the containing write root"
        );
        // And the rest of the session tree is still usable.
        let r = run(&ctx, "cd project && echo hi > work.txt && cat work.txt").await;
        assert!(
            !r.is_error && text_of(&r).contains("hi"),
            "the writable part of the tree must work: {:?}",
            text_of(&r)
        );
        let _ = base;
    }
}
