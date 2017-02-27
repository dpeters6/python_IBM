# Copyright 2015 IBM Corp. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os, json, pandas
from mysql import connector
from flask import Flask, jsonify

app = Flask(__name__)

vcap = json.loads(os.getenv("VCAP_SERVICES"))
creds = vcap['mysql'][0]['credentials']
SCHEMA = 'd4b34d227c2484ba6afcd7a02f3d7d977'

def get_mysql_conn():
    conn = connector.connect(host=creds['host'],
                             port=creds['port'],
                             user=creds['user'],
                             password=creds['password'])
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("USE {}".format(SCHEMA))
    return conn, cursor

def get_columns(table):
    conn, cursor = get_mysql_conn()
    cursor.execute("""SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
                      WHERE `TABLE_SCHEMA`='{}'
                      AND `TABLE_NAME`='BLUEMIX'""".format(SCHEMA))
    return cursor.fetchall()

def query_bluemix():
    conn, cursor = get_mysql_conn()
    cursor.execute("SELECT * FROM BLUEMIX")
    raw_results = cursor.fetchall()
    return pandas.DataFrame(raw_results)

@app.route('/')
def Welcome():
    return app.send_static_file('index.html')

@app.route('/myapp')
def WelcomeToMyapp():
    return 'Welcome again to my app running on Bluemix!'

@app.route('/api/people')
def GetPeople():
    list = [
        {'name': 'John', 'age': 28},
        {'name': 'Bill', 'val': 26}
    ]
    return jsonify(results=list)

@app.route('/api/people/<name>')
def SayHello(name):
    message = {
        'message': 'Hello ' + name
    }
    return jsonify(results=message)

@app.route('/mysql')
def showSql():
    df = query_bluemix()
    return df.to_html()

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port))
