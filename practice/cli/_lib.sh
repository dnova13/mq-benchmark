# 실습 스크립트 공통 함수. 각 스크립트가 source 한다.
# 단계 함수 이름은 step1, step2 ... 이고, STEPS 배열에 제목을 둔다.
title() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
note() { printf '\033[2m# %s\033[0m\n' "$*"; }
show() { printf '\033[36m$ %s\033[0m\n' "$*"; }   # 실행할 명령을 보여 준다

main() {
  case "${1:-list}" in
    list|-h|--help)
      echo "사용: $0 <단계번호|all|list>"
      for i in "${!STEPS[@]}"; do printf '  %d. %s\n' $((i + 1)) "${STEPS[$i]}"; done ;;
    all)
      for i in "${!STEPS[@]}"; do title "$((i + 1)). ${STEPS[$i]}"; "step$((i + 1))"; done ;;
    *)
      [ -n "${STEPS[$(( $1 - 1 ))]:-}" ] || { echo "없는 단계: $1"; exit 1; }
      title "$1. ${STEPS[$(( $1 - 1 ))]}"; "step$1" ;;
  esac
}
