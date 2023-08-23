import sqlalchemy
from sqlalchemy_serializer import SerializerMixin
from src.db_session import SqlAlchemyBase


class Ticker(SqlAlchemyBase, SerializerMixin):
    __tablename__ = 'ticker'
    __table_args__ = {'extend_existing': True}
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    name = sqlalchemy.Column(sqlalchemy.String)
    symbol = sqlalchemy.Column(sqlalchemy.String)
    aggregator = sqlalchemy.Column(sqlalchemy.String)
