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

import os, json, pandas, requests
from mysql import connector
from flask import Flask, jsonify, render_template

app = Flask(__name__, template_folder='static')

env_var = os.getenv("VCAP_SERVICES")
if env_var is not None:
    local = False
    vcap = json.loads(env_var)
    mysql_creds = vcap['mysql'][0]['credentials']
    lt_creds = vcap['language_translator'][0]['credentials']
    SCHEMA = 'd4b34d227c2484ba6afcd7a02f3d7d977'

elif env_var is None:
    local = True


def get_mysql_conn():
    conn = connector.connect(host=mysql_creds['host'],
                             port=mysql_creds['port'],
                             user=mysql_creds['user'],
                             password=mysql_creds['password'])
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("USE {}".format(SCHEMA))
    return conn, cursor


def get_columns(table):
    conn, cursor = get_mysql_conn()
    cursor.execute("""SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
                      WHERE `TABLE_SCHEMA`='{}'
                      AND `TABLE_NAME`='{}'""".format(SCHEMA, table))
    return [tup[0] for tup in cursor.fetchall()]


def query_bluemix(table):
    conn, cursor = get_mysql_conn()
    cursor.execute("SELECT * FROM {}".format(table))
    raw_results = cursor.fetchall()
    columns = get_columns(table)
    df = pandas.DataFrame(raw_results, columns=columns)
    return df


def translate_text(text, source, target):
    username = lt_creds['username']
    password = lt_creds['password']
    watsonUrl =  "{}/v2/translate?source={}&target={}&text={}".format(lt_creds['url'], source, target, text)
    try:
        r = requests.get(watsonUrl, auth=(username,password))
        return r.text
    except:
        return False

@app.route('/')
def Welcome():
    return app.send_static_file('index.html')


@app.route('/mysql')
def show_mysql():

    df = query_bluemix('BLUEMIX').to_csv('name_table.csv')#to_html(classes='testclass', index=False)
    #df = pandas.read_csv('name_table.csv').to_html(classes='testclass')
    return render_template('mysql.html', tables=[df], titles=['test_title'])

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port))
