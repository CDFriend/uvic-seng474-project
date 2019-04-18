import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.db_models import Base


def init_db(engine):
    Base.metadata.create_all(engine)


def get_db_session(db_name):
    # Assume sqlite for now, might have to add postgres support later
    engine = create_engine("sqlite:///%s" % db_name)
    init_db(engine)

    DBSession = sessionmaker(engine)
    DBSession.bind = engine

    return DBSession()


def model2dict(model, session):
    d = {}
    for column in model.__table__.columns:
        if len(column.foreign_keys) == 1:
            # Recursively traverse foreign keys
            id = getattr(model, column.name)
            foreign_key = next(key for key in column.foreign_keys)

            query = session.query(foreign_key.column.table).filter(foreign_key.column == id)

            col_name = re.sub("_Id$", "", column.name)
            if query.count() == 1:
                result = query.one()
                d[col_name] = _result_to_dict(query, result)
            else:
                d[col_name] = None

        elif len(column.foreign_keys) > 1:
            raise Exception("Can't handle more than one foreign key reference in a column!")
        else:
            d[column.name] = getattr(model, column.name)

    return d


def _result_to_dict(query, row):
    d = {}
    col_descs = query.column_descriptions
    for desc, item in zip(col_descs, row):
        d[desc["name"]] = item
    return d
