import sqlalchemy
from sqlalchemy.orm import relationship
from sqlalchemy_serializer import SerializerMixin
from src.db_session import SqlAlchemyBase


class TickerMeta(SqlAlchemyBase, SerializerMixin):
    __tablename__ = 'ticker_meta'
    __table_args__ = {'extend_existing': True}
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    ticker_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey("ticker.id"))
    timespan = sqlalchemy.Column(sqlalchemy.String)
    from_date = sqlalchemy.Column(sqlalchemy.Integer)
    to_date = sqlalchemy.Column(sqlalchemy.Integer)

    ticker = relationship("Ticker", backref="meta")
