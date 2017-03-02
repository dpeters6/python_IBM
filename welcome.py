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

import os, json, pandas, requests, logging
from mysql import connector
from flask import Flask, jsonify, render_template, request, redirect

app = Flask(__name__, template_folder='static')

env_var = os.getenv("VCAP_SERVICES")
live = bool(env_var)
if live:
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


def create_table(table):
    conn, cursor = get_mysql_conn()
    if table_exists(table):
        logging.warning('Table already exists')
        return
    cols = {'first_name': "VARCHAR(32)",
            'last_name': "VARCHAR(32)"}
    col_str = ', '.join([key + ' ' + cols[key] for key in cols])
    cursor.execute("""CREATE TABLE {}.{} ({})""".format(SCHEMA, table, col_str))
    conn.disconnect()


def drop_table(table):
    conn, cursor = get_mysql_conn()
    if table_exists(table):
        cursor.execute("""DROP TABLE {}.{}""".format(SCHEMA, table))
    conn.disconnect()


def table_exists(table):
    conn, cursor = get_mysql_conn()
    cursor.execute("""SELECT * FROM information_schema.tables
                      WHERE table_schema = '{}'
                      AND table_name = '{}'
                      LIMIT 1;""".format(SCHEMA, table))
    results = cursor.fetchall()
    return bool(results)


def reset_table(table):
    if table_exists(table):
        drop_table(table)
    create_table(table)

@app.route('/reset', methods=['GET', 'POST'])
def reset_table_from_html():
    if request.method == "POST" and live:
        reset_table('BLUEMIX')
        df = query_bluemix('BLUEMIX')
        html_table = df.to_html(classes='testclass', index=False)
        return render_template('mysql.html', tables=[html_table], titles=['test_title'])


def fake_reset(table):
    return str(table)


def get_columns(table):
    conn, cursor = get_mysql_conn()
    cursor.execute("""SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS`
                      WHERE `TABLE_SCHEMA`='{}'
                      AND `TABLE_NAME`='{}'""".format(SCHEMA, table))
    rawdata = cursor.fetchall()
    conn.disconnect()
    return [tup[0] for tup in rawdata]


def query_bluemix(table):
    conn, cursor = get_mysql_conn()
    cursor.execute("SELECT * FROM {}".format(table))
    raw_results = cursor.fetchall()
    columns = get_columns(table)
    df = pandas.DataFrame(raw_results, columns=columns)
    conn.disconnect()
    return df


def insert_into_bluemix(firstname, lastname):
    conn, cursor = get_mysql_conn()
    cursor.execute("INSERT INTO BLUEMIX (first_name, last_name) "
                   "VALUES ('{}', '{}')".format(firstname, lastname))
    conn.disconnect()


def translate_text(text, source, target):
    username = lt_creds['username']
    password = lt_creds['password']
    watsonUrl =  "{}/v2/translate?source={}&target={}&text={}".format(lt_creds['url'], source, target, text)
    try:
        r = requests.get(watsonUrl, auth=(username, password))
        return r.text
    except:
        return False

@app.route('/')
def Welcome():
    if live:
        reset_table('BLUEMIX')
    return render_template('index.html')

@app.route('/language_translator', methods=['GET', 'POST'])
def show_language_translator():
    languages = {'English': 'en', 'Spanish': 'es', 'French': 'fr', 'Arabic': 'ar'}
    if live:
        if request.method == "POST":
            data = request.form
            text = data['text']
            in_lang = data['input_language']
            out_lang = data['output_language']
            if in_lang == out_lang:
                translated = text
            else:
                translated = translate_text(text, in_lang, out_lang)
            return render_template('langtrans.html', translated=translated, languages=languages, def_text=text,
                                   prev_in=in_lang, prev_out=out_lang)
        else:
            return render_template('langtrans.html', languages=languages, def_text='', prev_in='en', prev_out='es')

    else:
        if request.method == "POST":
            data = request.form
            text = data['text']
            in_lang = data['input_language']
            out_lang = data['output_language']
            return render_template('langtrans.html', languages=languages, def_text=text, translated=text,
                                   prev_in=in_lang, prev_out=out_lang)
        else:
            return render_template('langtrans.html', languages=languages, prev_in='en', prev_out='es', def_text='')


@app.route('/mysql', methods=['GET', 'POST'])
def show_mysql():
    if request.method == "POST":
        text = request.form
        if live:
            # TODO sanitize text so no sql injection
            insert_into_bluemix(text['firstname'], text['lastname'])
        else:
            return "Success. First: {} Last: {}".format(text['firstname'], text['lastname'])
    if live:
        df = query_bluemix('BLUEMIX')
    else:
        df = pandas.read_csv('test.csv')
    html_table = df.to_html(classes='testclass', index=False)
    return render_template('mysql.html', tables=[html_table], titles=['test_title'], reset_table=lambda x: reset_table)

port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0', port=int(port))
