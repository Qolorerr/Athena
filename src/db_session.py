from pathlib import Path
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session
import sqlalchemy.ext.declarative as dec
from sqlalchemy.engine import Connection


SqlAlchemyBase = dec.declarative_base()
__engine = None
__factory = None


def global_init(db_file: Path):
    global __engine, __factory
    if __factory:
        return
    if not db_file.parent.exists():
        raise Exception("You need to set db file name")
    conn_str = f'sqlite:///{db_file}?check_same_thread=False'
    __engine = sa.create_engine(conn_str, echo=False)
    __factory = orm.sessionmaker(bind=__engine, expire_on_commit=False)
    import src.__all_models
    SqlAlchemyBase.metadata.create_all(__engine)


def create_session() -> Session:
    global __factory
    return __factory()


def create_connection() -> Connection:
    global __engine
    return __engine.connect()
