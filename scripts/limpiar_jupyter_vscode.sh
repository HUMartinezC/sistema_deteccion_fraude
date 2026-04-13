#!/usr/bin/env bash

set -euo pipefail

DRY_RUN=0
SKIP_KILL=0

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

if [[ "${1:-}" == "--skip-kill" ]] || [[ "${2:-}" == "--skip-kill" ]]; then
  SKIP_KILL=1
fi

run_cmd() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    eval "$@"
  fi
}

echo "=========================================="
echo " Limpieza de Jupyter + VS Code"
echo "=========================================="
echo "Dry run : $DRY_RUN"
echo "Skip kill: $SKIP_KILL"
echo

if [[ "$SKIP_KILL" -eq 0 ]]; then
  echo "[1/4] Cerrando procesos de Jupyter/ipykernel..."
  run_cmd "pkill -f 'jupyter-lab|jupyter-notebook|jupyter-server|ipykernel_launcher' || true"
else
  echo "[1/4] Se omite cierre de procesos (--skip-kill)."
fi

echo "[2/4] Limpiando runtime de Jupyter..."
JUPYTER_RUNTIME_DIR="${JUPYTER_RUNTIME_DIR:-$HOME/.local/share/jupyter/runtime}"
if [[ -d "$JUPYTER_RUNTIME_DIR" ]]; then
  run_cmd "find '$JUPYTER_RUNTIME_DIR' -maxdepth 1 -type f \\( -name 'kernel-*.json' -o -name 'nbserver-*.json' -o -name 'jpserver-*.json' -o -name '*.sock' \\) -print -delete"
else
  echo "No existe runtime dir: $JUPYTER_RUNTIME_DIR"
fi

echo "[3/4] Limpiando estado de Jupyter en VS Code (workspaceStorage)..."
WORKSPACE_STORAGES=(
  "$HOME/.config/Code/User/workspaceStorage"
  "$HOME/.config/Code - Insiders/User/workspaceStorage"
  "$HOME/.config/VSCodium/User/workspaceStorage"
)

for storage in "${WORKSPACE_STORAGES[@]}"; do
  if [[ -d "$storage" ]]; then
    run_cmd "find '$storage' -mindepth 2 -maxdepth 2 \\( -name 'ms-toolsai.jupyter*' -o -name 'ms-toolsai.vscode-jupyter*' \\) -print -exec rm -rf {} +"
  fi
done

echo "[4/4] Limpiando estado global de la extension Jupyter en VS Code..."
GLOBAL_STORAGES=(
  "$HOME/.config/Code/User/globalStorage"
  "$HOME/.config/Code - Insiders/User/globalStorage"
  "$HOME/.config/VSCodium/User/globalStorage"
)

for global_storage in "${GLOBAL_STORAGES[@]}"; do
  if [[ -d "$global_storage" ]]; then
    run_cmd "find '$global_storage' -mindepth 1 -maxdepth 1 \\( -name 'ms-toolsai.jupyter*' -o -name 'ms-toolsai.vscode-jupyter*' \\) -print -exec rm -rf {} +"
  fi
done

echo
echo "Limpieza completada."
echo "Siguiente paso recomendado: cerrar y reabrir VS Code, luego seleccionar kernel correcto."
