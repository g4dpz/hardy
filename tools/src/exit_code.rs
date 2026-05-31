/// Exit codes for `bp send` and `bp recv` tools.
///
/// These follow the conventions established in the design document:
/// - 0: Operation completed successfully
/// - 1: Bundle transfer was refused by the BPA (XFER_REFUSE)
/// - 2: Connection, session, file I/O, or configuration error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// Operation completed successfully
    Success = 0,
    /// Bundle transfer was refused by the BPA
    TransferRefused = 1,
    /// Connection/configuration/I/O error
    Error = 2,
}

impl From<ExitCode> for i32 {
    fn from(code: ExitCode) -> i32 {
        code as i32
    }
}

impl ExitCode {
    /// Exit the process with this exit code.
    pub fn exit(self) -> ! {
        std::process::exit(self as i32)
    }
}

/// Report an error to stderr and exit with [`ExitCode::Error`] (code 2).
///
/// This is the common path for connection failures, session errors, and file I/O errors.
pub fn report_error(msg: &str) -> ! {
    eprintln!("Error: {msg}");
    ExitCode::Error.exit()
}

/// Report a connection failure and exit with [`ExitCode::Error`] (code 2).
///
/// Format: `Error: Failed to connect to <addr>: <reason>`
pub fn report_connection_error(addr: &str, reason: &dyn std::fmt::Display) -> ! {
    report_error(&format!("Failed to connect to {addr}: {reason}"))
}

/// Report a TCPCLv4 session error and exit with [`ExitCode::Error`] (code 2).
///
/// Format: `Error: TCPCLv4 session terminated: <reason>`
pub fn report_session_error(reason: &dyn std::fmt::Display) -> ! {
    report_error(&format!("TCPCLv4 session terminated: {reason}"))
}

/// Report a bundle transfer refusal and exit with [`ExitCode::TransferRefused`] (code 1).
///
/// Format: `Error: Bundle transfer refused: <reason>`
pub fn report_transfer_refused(reason: &dyn std::fmt::Display) -> ! {
    eprintln!("Error: Bundle transfer refused: {reason}");
    ExitCode::TransferRefused.exit()
}

/// Report a file I/O error and exit with [`ExitCode::Error`] (code 2).
///
/// Format: `Error: Cannot read file '<path>': <reason>`
pub fn report_file_error(path: &std::path::Path, reason: &dyn std::fmt::Display) -> ! {
    report_error(&format!("Cannot read file '{}': {reason}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_values() {
        assert_eq!(ExitCode::Success as i32, 0);
        assert_eq!(ExitCode::TransferRefused as i32, 1);
        assert_eq!(ExitCode::Error as i32, 2);
    }

    #[test]
    fn exit_code_from_i32() {
        assert_eq!(i32::from(ExitCode::Success), 0);
        assert_eq!(i32::from(ExitCode::TransferRefused), 1);
        assert_eq!(i32::from(ExitCode::Error), 2);
    }
}
