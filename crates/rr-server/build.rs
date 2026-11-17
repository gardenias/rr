use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");
    println!("cargo:rerun-if-env-changed=RR_BUILD_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=RR_BUILD_GIT_DIRTY");

    let commit = std::env::var("RR_BUILD_GIT_COMMIT").unwrap_or_else(|_| {
        git_output(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".into())
    });
    let dirty = std::env::var("RR_BUILD_GIT_DIRTY").unwrap_or_else(|_| {
        git_output(&["status", "--porcelain", "--untracked-files=no"])
            .map(|output| (!output.is_empty()).to_string())
            .unwrap_or_else(|| "unknown".into())
    });
    println!("cargo:rustc-env=RR_BUILD_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=RR_BUILD_GIT_DIRTY={dirty}");
    println!(
        "cargo:rustc-env=RR_BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".into())
    );
}

fn git_output(arguments: &[&str]) -> Option<String> {
    let output = Command::new("git").args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}
