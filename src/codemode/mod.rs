//! Embedded JS engine for the codemode tool — the QuickJS sandbox (ADR 0089).

pub mod engine;

pub use engine::{race_turn, CodemodeEngine, EngineError, EngineLimits, ScriptEnd};
