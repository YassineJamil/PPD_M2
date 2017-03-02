# -*- coding: utf-8 -*-
"""
"""

import psycopg2
from switch import Switch
import re
import numpy as numpy
import sys
import pprint
import logging
import time

from flask import Flask, request, session, g, redirect, url_for, abort, \
    render_template, flash

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)

#File with disease's name
fmaladie_name = "maladies_bovines.txt"

def connect():
    return psycopg2.connect("dbname='"+ session['db_name'] +
                            "' user='"+ session['user'] +
                            "' host='" + session['host'] +
                            "' password='" + session['password'] + "'")

def drop_index_local():
        conn = connect()
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur1.execute(
            """
                SELECT table_name
                FROM INFORMATION_SCHEMA.TABLES
                WHERE table_name LIKE \'farm%\';
            """
        )
        tables = cur1.fetchall()
        #Please a good RegEx
        for table in tables:
            is_farm = re.compile('\\bfarm[0-9]+\\b', re.IGNORECASE)
            if (is_farm.match(table[0]) != None):
                print table[0]
            else:
                cur2.execute(
                    'drop table "' + table[0] + '";'
                    )
        conn.commit()

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/db_disconnect')
def db_disconnect():
    session.clear()
    return redirect(url_for('home'))

@app.route('/db_connect/', methods=['GET', 'POST'])
def db_connect():
    if not session.get('connexion'):
        if request.method == 'POST':
            db_name = request.form['db_name']
            usr = request.form['usr']
            psw = request.form['psw']
            #Localhost is default
            if request.form['host'] == "":
                host = "localhost"
            else:
                host = request.form['host']

            try:
                conn = psycopg2.connect("dbname='"+db_name+"' user='"+usr+"' host='"+host+"' password='"+psw+"'")
                conn.close()

            except:
                return redirect(url_for('error'))

            #set session data
            session['db_name'] = db_name
            session['user'] = usr
            session['password'] = psw
            session['host'] = host
            session['connexion'] = True
            return redirect(url_for('home'))

    else:
        return redirect(url_for('db_action'))

    return render_template("db_connect.html")

@app.route('/db_action', methods=['GET', 'POST'])
def db_action():
    if session.get('connexion'):
        if request.method == 'POST':
            with Switch(request.form['action']) as case:
                if case("top_K"):
                    return redirect(url_for("top_k"))
                if case("top_K_MD"):
                    return redirect(url_for("top_k_MD"))
                if case("top_K_Matrix"):
                    return redirect(url_for("top_k_matrix"))
                if case("threshold"):
                    return redirect(url_for("threshold"))
                if case("index_local"):
                    return redirect(url_for("index_local"))
                if case("index_global"):
                    return redirect(url_for("index_global"))
                if case("index_global_MD"):
                    return redirect(url_for("index_global_MD"))
                if case("index_global_matrix"):
                    return redirect(url_for("index_global_matrix"))
                if case("db_link"):
                    return redirect(url_for("db_link"))
                if case.default:
                    return redirect(url_for('error'))
        else:
            return render_template('home.html')
    else:
        return redirect(url_for('error'))

@app.route('/index/local')
def index_local():
    if session.get('connexion'):
        drop_index_local()
        conn = connect()
        cur1 = conn.cursor()
        cur2 = conn.cursor()

        with open(fmaladie_name) as f:
            maladies = (f.readlines())

        cur1.execute(
            'SELECT table_name '
            'FROM INFORMATION_SCHEMA.TABLES '
            'WHERE table_name LIKE \'farm%\';'
        )
        tables = cur1.fetchall()
        for table in tables:
            for maladie in maladies:
                cur2.execute(
                    'CREATE TABLE "' + table[0] + '_' + str(maladie).rstrip() + '" '
                    '(id INTEGER PRIMARY KEY, proba NUMERIC(10,8)); '
                    'INSERT INTO "' + table[0] + '_' + str(maladie).rstrip() + '" '
                    'SELECT id, proba '
                    'FROM "' + table[0] + '" '
                    'WHERE "maladie" '"= '" + (str(maladie).rstrip()) + "' "';'

                    'CREATE TABLE "test" (LIKE "' + table[0] + '_' + str(maladie).rstrip() + '"); '
                    'INSERT INTO "test" '
                    'SELECT * FROM "' + table[0] + '_' + str(maladie).rstrip() + '" ORDER BY proba DESC; '
                    'DROP TABLE "' + table[0] + '_' + str(maladie).rstrip() + '"; '
                    'ALTER TABLE "test" RENAME TO "' + table[0] + '_' + str(maladie).rstrip() + '";'
                )
        conn.commit()
        return render_template('index_local_confirm.html', active="index_local")
    else:
        return redirect(url_for('error'))

@app.route('/index_global', methods=['GET', 'POST'])
def index_global():
    if session.get('connexion'):
        if request.method == 'POST':
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            cur3 = conn.cursor()
            cur4 = conn.cursor()
            cur1.execute(
                "DROP TABLE IF EXISTS index_global;"
                "CREATE TABLE index_global(adress CHAR(100), proba NUMERIC(10,8));"
            )
            tab_ip = []
            sick = request.form['maladie']
            ip1 = request.form['ip1']
            tab_ip.append(ip1)
            ip2 = request.form['ip2']
            tab_ip.append(ip2)
            ip3 = request.form['ip3']
            tab_ip.append(ip3)
            doOrNo = request.form['choice']
            cur2.execute(
                #creation de table_path et la remplir
                'DROP TABLE IF EXISTS table_path;'
                'CREATE TABLE table_path(adress CHAR(100));'
                'insert into table_path (adress)'
                'SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE \'%_' + sick + '\';'
                """DROP TABLE IF EXISTS table_path_ip;
                CREATE TABLE table_path_ip(adress CHAR(100));"""
            )

            cur2.execute(
                """
                    SELECT adress FROM table_path
                """
            )
            listeFarms = cur2.fetchall()
            for farm in listeFarms:
                ipName = session['host'] + '-' + farm[0]
                cur2.execute(
                    """
                        INSERT INTO table_path_ip(adress) VALUES('""" + ipName + """')
                    """
                )

            cur3.execute(
                #pour remplir la listeTablesSick en bas
                """
                    SELECT adress FROM table_path;
                """
            )
            listeTablesSick = cur3.fetchall()
            for liste in listeTablesSick:
                cur3.execute(
                    #remplir l'index global
                    """
                        INSERT INTO index_global(adress, proba)
                        SELECT table_path_ip.adress, """+ liste[0].rstrip() +""".proba FROM table_path_ip, """+ liste[0].rstrip() +""" LIMIT 1;
                        DELETE FROM table_path WHERE ctid IN (SELECT ctid FROM table_path LIMIT 1);
                        DELETE FROM table_path_ip WHERE ctid IN (SELECT ctid FROM table_path_ip LIMIT 1)
                    """
                )
            cur4.execute(
                # mettre en ordre l'index global
                """
                    CREATE TABLE test (LIKE index_global);
                    INSERT INTO test
                    SELECT * FROM index_global ORDER BY proba DESC;
                    DROP TABLE index_global;
                    ALTER TABLE test RENAME TO index_global;
                """
                #Suppression de table_path et table_path_ip
                """
                    DROP TABLE IF EXISTS table_path;
                    DROP TABLE IF EXISTS table_path_ip;
                """
            )
            if (doOrNo == '1'):

                for ip in tab_ip:
                   # print ip
                    if ip:
                        # dblinK
                        cur4.execute(
                            """
                              SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                              INSERT INTO index_global
                              SELECT * FROM dblink('conx','SELECT * FROM index_global') AS t(a text ,c numeric );
                              CREATE TABLE test (LIKE index_global);
                              INSERT INTO test
                              SELECT * FROM index_global ORDER BY proba DESC;
                              DROP TABLE index_global;
                              ALTER TABLE test RENAME TO index_global;
                              SELECT dblink_disconnect('conx');

                            """
                        )
                    else :
                        print "pas de ip rentrer"

            conn.commit()
            return render_template('index_global_confirm.html', active="index_global")
        return render_template('index_global.html', active="index_global")
    else:
        return redirect(url_for('error'))

@app.route('/top_k', methods=['GET', 'POST'])
def top_k():
    if session.get('connexion'):
        if request.method == 'POST':
            start_time = time.time()
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            cur3 = conn.cursor()
            nbK = request.form['nb_k']
            nbKOfsset = int(nbK) - 1
            tab_kieme = []
            tab = []
            cur1.execute(
                """
                SELECT adress FROM index_global;
                """
            )
            liste = cur1.fetchall()
            for l in liste:
                adressFull = l[0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        SELECT proba FROM """+farm+""" LIMIT 1 offset """+str(nbKOfsset)+""" ;
                        """
                    )
                    tab_kieme.append(cur2.fetchall())
                else :
                    cur2.execute(
                        """
                        SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM dblink('conx','SELECT proba FROM """+farm+""" LIMIT 1 offset """+str(nbKOfsset)+"""') AS t(c numeric );
                        """
                    )
                    tab_kieme.append(cur2.fetchall())
                    cur2.execute(
                        """
                        SELECT dblink_disconnect('conx');
                        """
                    )
            tab_kieme.sort(reverse=True)
            delta_k_tab = tab_kieme[0]
            delta_k = delta_k_tab[0][0]
            cur3.execute(
                """
                DROP TABLE IF EXISTS top_k;
                CREATE TABLE top_k (LIKE index_global);
                DROP TABLE IF EXISTS help_proba;
                CREATE TABLE help_proba (proba NUMERIC(10,8));
                DROP TABLE IF EXISTS index_global_bis;
                CREATE TABLE index_global_bis (LIKE index_global);
                INSERT INTO index_global_bis
                SELECT * FROM  index_global WHERE proba>= """ + str(delta_k) + """;
                """
            )
            cur1.execute(
                """
                SELECT adress FROM index_global_bis
                """
            )
            listeTuples = cur1.fetchall()
            tab = []
            for tuple in listeTuples:
                adressFull = tuple[0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        TRUNCATE TABLE help_proba;
                        INSERT INTO help_proba
                        SELECT proba FROM """ + farm + """
                        WHERE proba>=
                        """ + str(delta_k) + """
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                else:
                    cur2.execute(
                        """
                        SELECT dblink_connect('conx', 'hostaddr=""" + ip + """ port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                            TRUNCATE TABLE help_proba;
                            INSERT INTO help_proba
                            SELECT * FROM dblink('conx','SELECT proba FROM """ + farm + """ WHERE proba>=""" + str(delta_k) + """ ') AS t(c numeric );
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                    cur2.execute(
                        """
                        SELECT dblink_disconnect('conx')
                        """
                    )
            tab.sort(reverse=True)
            tab_result = []
            for t in range(0, int(nbK)):
                tab_result.append(tab[t])
            cur1.execute(
                """
                DROP TABLE IF EXISTS top_k;
                DROP TABLE IF EXISTS help_proba;
                DROP TABLE IF EXISTS index_global_bis;
                """
            )
            conn.commit()
            execution_time = time.time() - start_time
            return render_template("topk_result.html", active="top_k", tab_result=tab_result, execution_time=execution_time)
        return render_template("top_k.html", active="top_k")

    else:
        return redirect(url_for('error'))

@app.route('/top_k_MD', methods=['GET', 'POST'])
def top_k_MD():
    if session.get('connexion'):
        if request.method == 'POST':
            start_time = time.time()
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            cur3 = conn.cursor()
            nbK = request.form['nb_k']
            nbKOfsset = int(nbK) - 1
            tab = []
            cur1.execute(
                """
                SELECT * FROM index_global_MD;
                """
            )
            #global uncertain index with MD content
            gui_MD = cur1.fetchall()
            nb_sites = len(gui_MD)
            final_result = 0
            #determine number of sites to loop through to get the k probabilities
            for j in range(1,nb_sites+1):
                if j < nb_sites and final_result <= int(nbK):
                    temp_result = 0
                    for i in range(1, j+1):
                        formula_response = (gui_MD[j-1][1] - gui_MD[j][1])/(gui_MD[i][2])
                        #print '(%f - %f)/%f' % (gui_MD[j-1][1],gui_MD[j][1],gui_MD[i][2])
                        temp_result += formula_response
                    final_result += temp_result
            #thanks to k number, find the probability at kth position of each farm present in the gui_MD
            tab_kieme = []
            index = 0
            while index < j:
                adressFull = gui_MD[index][0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        SELECT proba FROM """+farm+""" LIMIT 1 offset """+str(nbKOfsset)+""" ;
                        """
                    )
                    tab_kieme.append(cur2.fetchall())
                else :
                    cur2.execute(
                        """
                        SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM dblink('conx','SELECT proba FROM """+farm+""" LIMIT 1 offset """+str(nbKOfsset)+"""') AS t(c numeric );
                        """
                    )
                    tab_kieme.append(cur2.fetchall())
                    cur2.execute(
                        """
                        SELECT dblink_disconnect('conx');
                        """
                    )
                index += 1
            tab_kieme.sort(reverse=True)
            delta_k_tab = tab_kieme[0]
            delta_k = delta_k_tab[0][0]
            cur3.execute(
                """
                DROP TABLE IF EXISTS top_k_MD;
                CREATE TABLE top_k_MD (LIKE index_global_MD);
                DROP TABLE IF EXISTS help_proba;
                CREATE TABLE help_proba (proba NUMERIC(10,8));
                DROP TABLE IF EXISTS index_global_MD_bis;
                CREATE TABLE index_global_MD_bis (LIKE index_global_MD);
                INSERT INTO index_global_MD_bis
                SELECT * FROM  index_global_MD WHERE proba>= """ + str(delta_k) + """;
                """
            )
            cur1.execute(
                """
                SELECT adress FROM index_global_MD_bis
                """
            )
            listeTuples = cur1.fetchall()
            tab = []
            index = 0
            while index < j:
                adressFull = gui_MD[index][0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        TRUNCATE TABLE help_proba;
                        INSERT INTO help_proba
                        SELECT proba FROM """ + farm + """
                        WHERE proba>=
                        """ + str(delta_k) + """
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                else:
                    cur2.execute(
                        """
                        SELECT dblink_connect('conx', 'hostaddr=""" + ip + """ port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                            TRUNCATE TABLE help_proba;
                            INSERT INTO help_proba
                            SELECT * FROM dblink('conx','SELECT proba FROM """ + farm + """ WHERE proba>=""" + str(delta_k) + """ ') AS t(c numeric );
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                    cur2.execute(
                        """
                        SELECT dblink_disconnect('conx')
                        """
                    )
                index += 1
            tab.sort(reverse=True)
            tab_result = []
            for t in range(0, int(nbK)):
                tab_result.append(tab[t])
            cur1.execute(
                """
                DROP TABLE IF EXISTS top_k_MD;
                DROP TABLE IF EXISTS help_proba;
                DROP TABLE IF EXISTS index_global_MD_bis;
                """
            )
            conn.commit()
            execution_time = time.time() - start_time
            return render_template("topkMD_result.html", active="top_k_MD", tab_result=tab_result, execution_time=execution_time)
        return render_template("top_k_MD.html", active="top_k_MD")

    else:
        return redirect(url_for('error'))

@app.route('/top_k_matrix', methods=['GET', 'POST'])
def top_k_matrix():
    if session.get('connexion'):
        if request.method == 'POST':
            start_time = time.time()
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            cur3 = conn.cursor()
            nbK = request.form['nb_k']
            nbKOfsset = int(nbK) - 1
            tab_kieme = []
            tab = []
            cur1.execute(
                """
                SELECT matrix FROM index_global_matrix;
                """
            )
            GUI_matrix = cur1.fetchall()
            cleaned_GUI_matrix = [sub[0].rstrip().split("-") for sub in GUI_matrix]
            for index,sub in enumerate(cleaned_GUI_matrix):
                size = len(cleaned_GUI_matrix[0])-1
                if index > 0:
                    while len(sub) <= size+1:
                        sub.insert(0,'0')
            GUI_matrix = numpy.transpose(cleaned_GUI_matrix)
            counter = 0
            done = False
            for index, sub in enumerate(GUI_matrix):
                if counter <= int(nbK):
                   for item in sub:
                        counter = counter + int(item)
                else:
                    done == True
                    break
            all = False
            if index == len(GUI_matrix)-1:
               all = True
            cur1.execute(
                """
                SELECT adress FROM index_global_matrix;
                """
            )
            liste = cur1.fetchall()
            for i, l in enumerate(liste):
                if i == index and not all:
                    break
                adressFull = l[0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        SELECT proba FROM """+farm+""" LIMIT 1 offset """+str(nbKOfsset)+""" ;
                        """
                    )
                    tab_kieme.append(cur2.fetchall())
                else :
                    cur2.execute(
                        """
                        SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM dblink('conx','SELECT proba FROM """+farm+""" LIMIT 1 offset """+str(nbKOfsset)+"""') AS t(c numeric );
                        """
                    )
                    tab_kieme.append(cur2.fetchall())
                    cur2.execute(
                        """
                        SELECT dblink_disconnect('conx');
                        """
                    )
            tab_kieme.sort(reverse=True)
            delta_k_tab = tab_kieme[0]
            delta_k = delta_k_tab[0][0]
            cur3.execute(
                """
                DROP TABLE IF EXISTS top_k;
                CREATE TABLE top_k (LIKE index_global_matrix);
                DROP TABLE IF EXISTS help_proba;
                CREATE TABLE help_proba (proba NUMERIC(10,8));
                DROP TABLE IF EXISTS index_global_matrix_bis;
                CREATE TABLE index_global_matrix_bis (LIKE index_global_matrix);
                INSERT INTO index_global_matrix_bis
                SELECT * FROM  index_global_matrix WHERE proba>= """ + str(delta_k) + """;
                """
            )
            cur1.execute(
                """
                SELECT adress FROM index_global_matrix_bis
                """
            )
            listeTuples = cur1.fetchall()
            tab = []
            for i,tuple in enumerate(listeTuples):
                if i == index and not all:
                    break
                adressFull = tuple[0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        TRUNCATE TABLE help_proba;
                        INSERT INTO help_proba
                        SELECT proba FROM """ + farm + """
                        WHERE proba>=
                        """ + str(delta_k) + """
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                else:
                    cur2.execute(
                        """
                        SELECT dblink_connect('conx', 'hostaddr=""" + ip + """ port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                            TRUNCATE TABLE help_proba;
                            INSERT INTO help_proba
                            SELECT * FROM dblink('conx','SELECT proba FROM """ + farm + """ WHERE proba>=""" + str(delta_k) + """ ') AS t(c numeric );
                        """
                    )
                    cur2.execute(
                        """
                        SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                    cur2.execute(
                        """
                        SELECT dblink_disconnect('conx')
                        """
                    )
            tab.sort(reverse=True)
            tab_result = []
            for t in range(0, int(nbK)):
                tab_result.append(tab[t])
            cur1.execute(
                """
                DROP TABLE IF EXISTS top_k;
                DROP TABLE IF EXISTS help_proba;
                DROP TABLE IF EXISTS index_global_matrix_bis;
                """
            )
            conn.commit()
            execution_time = time.time() - start_time
            return render_template("topk_matrix_result.html", active="top_k_matrix", tab_result=tab_result, execution_time=execution_time)
        return render_template("top_k_matrix.html", active="top_k_matrix")

    else:
        return redirect(url_for('error'))

@app.route('/threshold', methods=['GET', 'POST'])
def threshold():
    if session.get('connexion'):
        if request.method == 'POST':
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            seuil_check = request.form['seuil']

            cur1.execute(
                """

                DROP TABLE IF EXISTS threshold;
                CREATE TABLE threshold (LIKE index_global);
                DROP TABLE IF EXISTS help_proba;
                CREATE TABLE help_proba (proba NUMERIC(10,9));
                DROP TABLE IF EXISTS index_global_bis;
                CREATE TABLE index_global_bis (LIKE index_global);
                INSERT INTO index_global_bis
                SELECT * FROM  index_global WHERE proba>= """+seuil_check+""";

                """
            )
            cur1.execute(
                """
                    SELECT adress FROM index_global_bis
                """
            )
            listeTuples = cur1.fetchall()
            tab = []
            for tuple in listeTuples:
                adressFull = tuple[0].rstrip()
                tabAdressFull = adressFull.split("-")
                ip = tabAdressFull[0]
                farm = tabAdressFull[1]
                if (ip == 'localhost'):
                    cur2.execute(
                        """
                        TRUNCATE TABLE help_proba;
                        INSERT INTO help_proba
                        SELECT proba FROM """+farm+"""
                        WHERE proba>=
                        """+seuil_check+"""
                        """
                    )
                    cur2.execute(
                        """
                            SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0],ip, farm]
                        tab.append(test)
                else:

                    cur2.execute(
                        """
                            SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                        """
                    )
                    cur2.execute(
                        """
                            TRUNCATE TABLE help_proba;
                            INSERT INTO help_proba
                            SELECT * FROM dblink('conx','SELECT proba FROM """ + farm + """ WHERE proba>=""" + seuil_check + """ ') AS t(c numeric );
                        """
                    )
                    cur2.execute(
                        """
                            SELECT * FROM help_proba
                        """
                    )
                    proba = cur2.fetchall()
                    for p in proba:
                        test = [p[0], ip, farm]
                        tab.append(test)
                    cur2.execute(
                        """
                            SELECT dblink_disconnect('conx')
                        """
                    )
                tab.sort(reverse=True)
            cur1.execute(
                """
                DROP TABLE IF EXISTS threshold;
                DROP TABLE IF EXISTS help_proba;
                DROP TABLE IF EXISTS index_global_bis;
                """
            )
            conn.commit()
            return render_template("threshold_result.html", active="threshold", res=tab)
        return render_template("threshold.html", active="threshold")
    else:
        return redirect(url_for('error'))

@app.route('/index_global_MD', methods=['GET', 'POST'])
def index_global_MD():
    if session.get('connexion'):
        if request.method == 'POST':
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            cur3 = conn.cursor()
            cur4 = conn.cursor()
            cur1.execute(
                "DROP TABLE IF EXISTS index_global;"
                "CREATE TABLE index_global(adress CHAR(100), proba NUMERIC(10,8));"
                "DROP TABLE IF EXISTS index_global_MD;"
                "CREATE TABLE index_global_md(adress CHAR(100), proba NUMERIC(10,8), md NUMERIC (10,8));"
            )
            tab_ip = []
            sick = request.form['maladie']
            ip1 = request.form['ip1']
            tab_ip.append(ip1)
            ip2 = request.form['ip2']
            tab_ip.append(ip2)
            ip3 = request.form['ip3']
            tab_ip.append(ip3)
            doOrNo = request.form['choice']
            cur2.execute(
                # creation de table_path et la remplir
                'DROP TABLE IF EXISTS table_path;'
                'CREATE TABLE table_path(adress CHAR(100));'
                'insert into table_path (adress)'
                'SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE \'%_' + sick + '\';'
                """DROP TABLE IF EXISTS table_path_ip;
                CREATE TABLE table_path_ip(adress CHAR(100));"""
            )

            cur2.execute(
                """SELECT adress FROM table_path"""
            )
            listeFarms = cur2.fetchall()
            for farm in listeFarms:
                ipName = session['host'] + '-' + farm[0]
                cur2.execute(
                    """
                    INSERT INTO table_path_ip(adress) VALUES('""" + ipName + """')"""
                )

            cur3.execute(
                # pour remplir la listeTablesSick en bas
                'SELECT adress FROM table_path;'
            )
            listeTablesSick = cur3.fetchall()
            for liste in listeTablesSick:
                cur3.execute(
                    # remplir l'index global
                    """INSERT INTO index_global(adress, proba)
                    SELECT table_path_ip.adress, """ + liste[0].rstrip() + """.proba FROM table_path_ip, """ + liste[
                        0].rstrip() + """ LIMIT 1;
                    DELETE FROM table_path WHERE ctid IN (SELECT ctid FROM table_path LIMIT 1);
                    DELETE FROM table_path_ip WHERE ctid IN (SELECT ctid FROM table_path_ip LIMIT 1)"""
                )
            cur4.execute(
                # mettre en ordre l'index global
                """CREATE TABLE test (LIKE index_global);
                    INSERT INTO test
                    SELECT * FROM index_global ORDER BY proba DESC;
                    DROP TABLE index_global;
                    ALTER TABLE test RENAME TO index_global;
                """
                # Suppression de table_path et table_path_ip
                """
                DROP TABLE IF EXISTS table_path;
                DROP TABLE IF EXISTS table_path_ip;
                """
            )
            #index global créé, passons maintenant à l'index global MD
            #en local ou distribué ? Si doOrNo = 1 oui sinon on passe au else
            if (doOrNo == '1'):
                for ip in tab_ip:
                    #print ip
                    if ip:
                        # dblinK
                        cur4.execute(
                            """
                              SELECT dblink_connect('conx', 'hostaddr=""" + ip + """ port=5432 dbname=PPD user=postgres password=root');
                              INSERT INTO index_global
                              SELECT * FROM dblink('conx','SELECT * FROM index_global') AS t(a text ,c numeric );
                              CREATE TABLE test (LIKE index_global);
                              INSERT INTO test
                              SELECT * FROM index_global ORDER BY proba DESC;
                              DROP TABLE index_global;
                              ALTER TABLE test RENAME TO index_global;
                              SELECT dblink_disconnect('conx');
                            """
                        )
                    else:
                        print "pas de ip rentrer"
            else :
                cur4.execute(
                    """
                    SELECT * FROM index_global;
                    """
                )
                list_index_global = cur4.fetchall()
                final_proba_list = []
                #pour chaque farm trouvée dans l'index global, on fait des traitements
                for list in list_index_global:
                    adressFull = list[0].rstrip()
                    tabAdressFull = adressFull.split("-")
                    ip = tabAdressFull[0]
                    farm = tabAdressFull[1]
                    prb = list[1]
                    f_prb = float(prb)
                    #récupération des données de la farm lue
                    cur1.execute(
                        """
                        SELECT proba FROM """+ farm +""";
                        """
                    )
                    test = cur1.fetchall()
                    res = 0
                    pb = 0
                    i = 1
                    end = len(test)
                    #calcul de la MD pour chacune des probas des entrées de la farm
                    for index, t in enumerate(test) :
                        j = index + 1
                        if(j == 1) :
                            proba_max = float(t[0])
                            continue
                        current_proba = float(t[0])
                        #print '(1/(%i))*((%f-%f)/(%i-1))' % (len(test)-1, proba_max, current_proba, j)
                        dispersion_measure = (1.0/(len(test)-1))*((proba_max - current_proba)/float((j-1)))
                        #print dispersion_measure
                    # for t in test:
                        # if (i < end):
                        #     cur2.execute(
                        #         """
                        #         SELECT proba FROM """ + farm + """ LIMIT 1 OFFSET """+ str(i) + """;
                        #         """
                        #     )
                        #     pb_sous = cur2.fetchall()
                        #     f_pb_sous = float(pb_sous[0][0])
                        #     res = res + ((f_prb - f_pb_sous )/i)
                        #     i = i + 1
                        # else :
                        #     res = res / (i - 1)
                    cur3.execute(
                        """
                        INSERT INTO index_global_md(adress, proba, md)
                        VALUES ('"""+ str(adressFull) +"""', '"""+ str(prb) +"""', '"""+ str(dispersion_measure) +"""');
                        """
                    )
            conn.commit()
            return render_template('index_global_MD_confirm.html', active="index_global_MD")
        return render_template('index_global_MD.html', active="index_global_MD")
    else:
        return redirect(url_for('error'))

@app.route('/index_global_matrix', methods=['GET', 'POST'])
def index_global_matrix():
    if session.get('connexion'):
        if request.method == 'POST':
            conn = connect()
            cur1 = conn.cursor()
            cur2 = conn.cursor()
            cur3 = conn.cursor()
            cur4 = conn.cursor()
            cur1.execute(
                "DROP TABLE IF EXISTS index_global_matrix;"
                "CREATE TABLE index_global_matrix_temp(adress CHAR(100), proba NUMERIC(10,8));"
               )
            tab_ip = []
            sick = request.form['maladie']
            ip1 = request.form['ip1']
            tab_ip.append(ip1)
            ip2 = request.form['ip2']
            tab_ip.append(ip2)
            ip3 = request.form['ip3']
            tab_ip.append(ip3)
            doOrNo = request.form['choice']
            cur2.execute(
                #creation de table_path et la remplir
                'DROP TABLE IF EXISTS table_path;'
                'CREATE TABLE table_path(adress CHAR(100));'
                'insert into table_path (adress)'
                'SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name LIKE \'%_' + sick + '\';'
                """DROP TABLE IF EXISTS table_path_ip;
                CREATE TABLE table_path_ip(adress CHAR(100));"""
            )

            cur2.execute(
                """
                    SELECT adress FROM table_path
                """
            )
            listeFarms = cur2.fetchall()
            for farm in listeFarms:
                ipName = session['host'] + '-' + farm[0]
                cur2.execute(
                    """
                        INSERT INTO table_path_ip(adress) VALUES('""" + ipName + """')
                    """
                )

            cur3.execute(
                #pour remplir la listeTablesSick en bas
                """
                    SELECT adress FROM table_path;
                """
            )
            listeTablesSick = cur3.fetchall()
            for liste in listeTablesSick:
                cur3.execute(
                    #remplir l'index global
                    """
                        INSERT INTO index_global_matrix_temp(adress, proba)
                        SELECT table_path_ip.adress, """+ liste[0].rstrip() +""".proba FROM table_path_ip, """+ liste[0].rstrip() +""" LIMIT 1;
                        DELETE FROM table_path WHERE ctid IN (SELECT ctid FROM table_path LIMIT 1);
                        DELETE FROM table_path_ip WHERE ctid IN (SELECT ctid FROM table_path_ip LIMIT 1)
                    """
                )
            cur4.execute(
                # mettre en ordre l'index global
                """
                    CREATE TABLE test (LIKE index_global_matrix_temp);
                    INSERT INTO test
                    SELECT * FROM index_global_matrix_temp ORDER BY proba DESC;
                    DROP TABLE index_global_matrix_temp;
                    ALTER TABLE test RENAME TO index_global_matrix_temp;
                """
                #Suppression de table_path et table_path_ip
                """
                    DROP TABLE IF EXISTS table_path;
                    DROP TABLE IF EXISTS table_path_ip;
                """
            )
            cur4.execute(
                """
                CREATE TABLE index_global_matrix(adress CHAR(100), proba NUMERIC(10,8), matrix CHAR(100));
                SELECT * FROM index_global_matrix_temp;
                """
            )
            GUI_content = cur4.fetchall()
            nb_sites = len(GUI_content)
            matrix = []
            # for each site in GUI, we search all probabilities between each max_proba of each site (cf matrix algo)
            for index, site in enumerate(GUI_content):
                """verify the algorithm : maybe there is an error"""
                if index < nb_sites-1:
                    for i in range(index+1, nb_sites):
                        max_proba = str(GUI_content[i-1][1])
                        min_proba = str(GUI_content[i][1])
                        address_site = site[0].rstrip()
                        address_site = address_site.split("-")
                        ip = address_site[0]
                        farm = address_site[1]
                        #get the number of probas between max_proba and min_proba
                        if (ip == 'localhost'):
                            cur2.execute(
                                """
                                SELECT COUNT (proba) FROM """+farm+""" WHERE proba <= """ + max_proba + """ AND proba >= """ + min_proba + """ ;
                                """
                            )
                            nb = cur2.fetchone()
                            matrix.append(nb[0])
                        else :
                            cur2.execute(
                                """
                                SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                                """
                            )
                            cur2.execute(
                                """
                                SELECT * FROM dblink('conx','SELECT COUNT (proba) FROM """+farm+""" WHERE proba <= """ + max_proba + """ AND proba >= """ + min_proba + """ ;') AS t(c numeric );
                                """
                            )
                            nb = cur2.fetchone()
                            matrix.append(nb[0])
                            cur2.execute(
                                """
                                SELECT dblink_disconnect('conx');
                                """
                            )
                    #formatting of matrix by a string of character in order to ease future topk_matrix operations
                    string_matrix = ''
                    for i, number in enumerate(matrix):
                        if i < len(matrix) - 1:
                            string_matrix += '%i-' % number
                        else:
                            string_matrix += '%i' % number
                    site = site + (string_matrix,)
                else:
                     string_matrix = '0'
                #print "%s, %s, %s" % (site[0].strip(), str(site[1]), string_matrix)
                #insertion in the definite table
                cur4.execute(
                    """
                    INSERT INTO index_global_matrix(adress, proba, matrix)
                    VALUES ('"""+ site[0].strip() +"""', '"""+ str(site[1]) +"""', '"""+ string_matrix +"""');
                    DROP TABLE IF EXISTS index_global_matrix_temp;
                    """
                )
                matrix = []
            if (doOrNo == '1'):
                for ip in tab_ip:
                    if ip:
                        # dblinK
                        cur4.execute(
                            """
                              SELECT dblink_connect('conx', 'hostaddr="""+ip+""" port=5432 dbname=PPD user=postgres password=root');
                              INSERT INTO index_global_matrix
                              SELECT * FROM dblink('conx','SELECT * FROM index_global_matrix') AS t(a text ,c numeric );
                              CREATE TABLE test (LIKE index_global_matrix);
                              INSERT INTO test
                              SELECT * FROM index_global_matrix ORDER BY proba DESC;
                              DROP TABLE index_global_matrix;
                              ALTER TABLE test RENAME TO index_global_matrix;
                              SELECT dblink_disconnect('conx');

                            """
                        )
                    else :
                        print "pas de ip rentrer"

            conn.commit()
            return render_template('index_global_matrix_confirm.html', active="index_global_matrix")
        return render_template('index_global_matrix.html', active="index_global_matrix")
    else:
        return redirect(url_for('error'))

@app.route('/error')
def error():
    return render_template("db_error.html")

app.secret_key = '6Mb0JgsZ4HLJpnbSjNiZuTwaSA7RwV3Inf6DUmnu'

if __name__ == '__main__':
    app.run()
