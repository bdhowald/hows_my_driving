import traffic_violations.db.database as db

class Base(db.DeclarativeBase):
    __abstract__ = True

    @classmethod
    def get_by(cls, **kwargs):
        assert kwargs, 'kwargs can\'t be empty'
        return cls.query.filter_by(**kwargs).first()

    @classmethod
    def get_all_by(cls, **kwargs):
        assert kwargs, 'kwargs can\'t be empty'
        return cls.query.filter_by(**kwargs).all()

    @classmethod
    def get_all_in(cls, **kwargs):
        assert kwargs, 'kwargs can\'t be empty'
        query_tuples = [getattr(cls, k).in_(v) for k, v in kwargs.items()]
        return cls.query.filter(*query_tuples).all()

    @classmethod
    def iter_all(cls):
        return cls.query