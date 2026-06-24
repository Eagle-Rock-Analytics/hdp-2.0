#!/usr/bin/env bash
# setup_head_node.sh
#
# Run this ONCE after SSHing into the freshly created pcluster head node.
# It installs uv, clones hdp-2.0, and builds the Python venv.
#
# The /home/ec2-user directory is automatically NFS-exported to all compute
# nodes by pcluster — no EFS required. The .venv lives here too.
#
# Usage (from your laptop):
#   ssh -i ~/.ssh/neil-caladapt.pem ec2-user@<HEAD_NODE_PUBLIC_IP>
#   bash setup_head_node.sh
#
# After this script completes, submit jobs from scripts/pcluster/:
#   cd ~/hdp-2.0/scripts/pcluster
#   sbatch run_pull_ASOSAWOS.sh
#   # wait for completion, then:
#   sbatch run_clean_ASOSAWOS.sh
#   # wait, then:
#   sbatch run_qaqc_ASOSAWOS.sh
#   # wait, then:
#   sbatch run_merge_append_ASOSAWOS.sh

set -euo pipefail

REPO_ROOT=/home/ec2-user/hdp-2.0

echo "=== [1/4] Installing uv ==="
curl -LsSf https://astral.sh/uv/install.sh | sh
# Make uv available in current shell
export PATH="$HOME/.local/bin:$PATH"

echo "=== [2/4] Installing git and system deps ==="
sudo dnf install -y git gcc

echo "=== [3/4] Cloning hdp-2.0 ==="
if [ -d "${REPO_ROOT}" ]; then
  echo "Repo already exists — pulling latest."
  cd "${REPO_ROOT}" && git pull --rebase
else
  git clone https://github.com/Eagle-Rock-Analytics/hdp-2.0.git "${REPO_ROOT}"
fi

echo "=== [4/4] Installing Python environment via uv ==="
cd "${REPO_ROOT}"
uv sync

echo ""
echo "=== Setup complete ==="
echo "Venv at: ${REPO_ROOT}/.venv"
echo ""
echo "Next steps:"
echo "  cd ${REPO_ROOT}/scripts/pcluster"
echo "  sbatch run_pull_ASOSAWOS.sh      # P1.4 GHCNh pull"
echo "  # (wait for completion)"
echo "  sbatch run_clean_ASOSAWOS.sh     # P1.5a GHCNh clean"
echo "  # (wait for completion)"
echo "  sbatch run_qaqc_ASOSAWOS.sh      # P1.5b QAQC append"
echo "  # (wait for completion)"
echo "  sbatch run_merge_append_ASOSAWOS.sh  # hdp-b1d.10 merge"
