import main.ConnectionPool as cp
from psycopg2 import DatabaseError
import pandas as pd
'''
Bulk data selection and insert process.
Selection process includes "select top n or all rows" queries.
'''
class Query:
    title = ['datetime', 'clickid', 'userid', 'usersessionid', 'ishit', 'teamid', 'teamlevel']
    data = None

    def __init__(self, count=None):
        self.cnt = count

    def retrieve_data(self, top_count=None):
        with cp.ConnectionCursor() as cursor:
            if top_count is None or top_count == '*':
                cursor.execute('select datetime, clickid, userid, usersessionid, ishit, teamid, teamlevel from game_clicks;')
            else:
                cursor.execute('select datetime, clickid, userid, usersessionid, ishit, teamid, teamlevel from game_clicks limit %s;', (top_count,))

            record = cursor.fetchall()
            return record

    def to_dataframe(self,data):
        df=pd.DataFrame(data=data,columns=self.title)
        return df

    @classmethod
    def bulk_insert(cls, dataframe):
        cls.data=dataframe
        cols=",".join([str(c) for c in cls.title])
        try:
            with cp.ConnectionCursor() as cursor:
                cursor.execute("truncate table game_clicks;")
                for i, row in dataframe.iterrows():
                    sql = "INSERT INTO game_clicks (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s);"
                    cursor.execute(sql, tuple(row))
            print("The data integration has been successfully completed.")
        except(Exception, DatabaseError) as error:
            print('Some error(s) occured while data integration.\n ERROR: {}'.format(error))