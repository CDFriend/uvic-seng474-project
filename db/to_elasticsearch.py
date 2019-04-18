import sys
import os
import json
from db.db_utils import get_db_session, model2dict
from db.db_models import Person


INDEX_NAME = "people"


def transfer_to_elastic(session):
    for person in session.query(Person):
        # Write index action and ID
        # See Elasticsearch bulk API reference:
        #   https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        json.dump({"index": {"_index": INDEX_NAME, "_id": person.Id, "_type": "_doc"}}, sys.stdout)
        sys.stdout.write("\n")

        json.dump(model2dict(person, session), sys.stdout)
        sys.stdout.write("\n")


if __name__ == "__main__":
    session = get_db_session(sys.argv[1])
    transfer_to_elastic(session)
