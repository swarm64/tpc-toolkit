# set -x

intexit() {
    kill -HUP -$$
}

hupexit () {
    echo
    echo "Interrupted"
    exit
}

trap hupexit HUP
trap intexit INT

set -e
DB_HOST="localhost"
DB_PORT=5432
NUM_PARTITIONS=32
CHUNKS=10
RMQ_DSN=amqp://localhost

function print_help {
echo "
Usage instructions:
    $0 \\
      --dbname=<db to use> \\
      --schema=<schema-to-use> \\
      --scale-factor=<scale-factor-to-use>

    Optional arguments:
      --db-host         The host of the database to connect to.
                        Default: localhost

      --db-port         The port of the database to connec to.
                        Default: 5432

      --num-partitions  How many partitions to use (if applicable)
                        Default: 32

      --chunks          How many parallel data chunks to generate
                        Default: 10"
}

for i in "$@"
do
case $i in
    --num-partitions=*)
    NUM_PARTITIONS="${i#*=}"
    shift
    ;;
    --schema=*)
    SCHEMA="${i#*=}"
    shift
    ;;
    --scale-factor=*)
    SCALE_FACTOR="${i#*=}"
    shift
    ;;
    --chunks=*)
    CHUNKS="${i#*=}"
    shift
    ;;
    --dbname=*)
    DB="${i#*=}"
    shift
    ;;
    --db-host=*)
    DB_HOST="${i#*=}"
    shift
    ;;
    --db-port=*)
    DB_PORT="${i#*=}"
    shift
    ;;
    --nodes=*)
    IFS=',' read -r -a NODES <<< "${i#*=}"
    shift
    ;;
    --rmq-dsn=*)
    RMQ_DSN="${i#*=}"
    shift
    ;;
    *)
        echo "Unknown option $i"
        print_help
        exit -1
    ;;
esac
done

PSQL="psql -U postgres -p ${DB_PORT} -h"

function check_and_fail {
    if [ -z ${!1+x} ]; then
        echo "ERROR: ${2} is not set."
        print_help
        exit -1
    fi
}

check_and_fail DB "--dbname"
check_and_fail SCHEMA "--schema"
check_and_fail SCALE_FACTOR "--scale-factor"
check_and_fail NODES "--nodes"
check_and_fail RMQ_DSN "--rmq-dsn"

function check_program_and_fail {
    PROGRAM=$1
    HINT=$2
    if ! hash $PROGRAM 2> /dev/null; then
        echo "ERROR:    $PROGRAM    is not installed. $HINT"
        exit -1
    fi
}

check_program_and_fail "jinja2" "Did you run 'pip3 install -r requirements.txt'?"
check_program_and_fail "psql" "Is it installed? Is PATH setup properly?"

function wait_for_pg {
    set +e

    PSQL_UP=0
    for i in {0..120}; do
        $PSQL $1 -d postgres -c 'SELECT 1' &> /dev/null
        if [ $? -eq 0 ]; then
            PSQL_UP=1
            break
        fi
        sleep 1
    done

    if [ $PSQL_UP -ne 1 ]; then
        echo "PSQL did not come up."
        exit -1
    fi

    set -e
}

function psql_exec_file {
    $PSQL $1 -d $DB -f "$2"
}

function psql_exec_cmd {
    $PSQL $2 -d $DB -c "$1"
}

function prepare_db {
    HOST=$1
    ENCODING=$2
    echo "Preparing DB ${DB} on ${HOST}"
    $PSQL $HOST -c "DROP DATABASE IF EXISTS $DB"
    $PSQL $HOST -c "CREATE DATABASE $DB WITH ENCODING ${ENCODING} TEMPLATE TEMPLATE0"
}

function run_if_exists {
    FILE=$SCHEMA/$1
    NODE=$2
    if [ -f "$FILE" ]; then
        echo "Executing $FILE"
        psql_exec_file $NODE $FILE
    fi
}

function deploy_schema {
    NODE=$1
    NODE_IDX=$2
    NODES=$3
    SCHEMA=$4
    NUM_PARTITIONS=$5
    SCHEMA_FILE=`mktemp`
    jinja2 $SCHEMA/schema.sql -D nodes=$NODES -D node=$NODE_IDX -D partitions=$NUM_PARTITIONS > $SCHEMA_FILE
    psql_exec_file $NODE $SCHEMA_FILE
}
