set -a
. ./env
set +a

python manage.py "$@"