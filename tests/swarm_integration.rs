use std::process::Command;

#[test]
#[ignore = "requires docker and builds a 10-node combined swarm"]
fn swarm_replication_and_notifications() {
    let status = Command::new("bash")
        .arg("tests/swarm_integration.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .expect("failed to launch swarm integration script");

    assert!(status.success(), "swarm integration script failed");
}
