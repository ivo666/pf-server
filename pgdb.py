import psycopg2

class pgDatabase:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        
        self.connection = psycopg2.connect(
            host = host,
            user = user, 
            database = database, 
            password = password
            )
        
        self.cursor = self.connection.cursor()
        self.connection.autocommit = True
        
    def post(self, query, args):
        try:
            self.cursor.execute(query, args)
        except Exception as err:
            print(repr(err))
