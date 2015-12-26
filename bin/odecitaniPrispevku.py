#! /usr/bin/python
# -*- coding: utf-8 -*-
#
# Author: Kendy 11/2015
# Verze: 1.0
#
#
#

import ConfigParser, os, sys, re
import MySQLdb as mdb
from datetime import datetime
from optparse import OptionParser

VERSION = '1.0.0'

# Aktualni pracovni adresar scriptu, s nim si automaticky nactem konfigurak
workdir = os.path.dirname(os.path.realpath(__file__))
config_file = workdir + '/../etc/money.ini'

# Nacist ini
if os.path.exists(config_file):
	try:
		config = ConfigParser.RawConfigParser()
		config.read(config_file)
		mysql_dbhost	= config.get('mysql', 'dbhost')
		mysql_dbname	= config.get('mysql', 'dbname')
		mysql_dbuser	= config.get('mysql', 'dbuser')
		mysql_dbpass	= config.get('mysql', 'dbpass')
		vysePrispevku	= config.get('ucty', 'prispevky')
		vysePrispevku	= int(vysePrispevku)
	except ConfigParser.ParsingError:
		print "Nemohu rozparsovat konfiguracni soubor " + config_file
		sys.exit(1)
else:
	print "Nemohu nalezt konfiguracni soubor " + config_file
	sys.exit(1)

##################################################################################################################
# Hlavni program zde
def main(argv):
	# Zde se delaji operace nad db, takze pokud se nepripojime, nema cenu dal pokracovat
	try:
		con = mdb.connect(host=mysql_dbhost, user=mysql_dbuser, passwd=mysql_dbpass, db=mysql_dbname, use_unicode=True, charset="utf8")
	except mdb.Error, e:
		print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
		print "Nemohu se pripojit do databaze host:[%s] user:[%s] heslo:[*****] dbname:[%s]" % (mysql_dbhost, mysql_dbuser, mysql_dbname)
		sys.exit(1)

	# Aktualni cas
	mesicNyni	= "%02d" % int(datetime.today().month)
	denNyni		= "%02d" % int(datetime.today().day)
	hodinaNyni	= "%02d" % int(datetime.today().hour)
	minutaNyni	= "%02d" % int(datetime.today().minute)
	sekundaNyni	= "%02d" % int(datetime.today().second)
	datetimeTed = ( "%s-%s-%s %s:%s:%s" ) % (datetime.today().year, mesicNyni, denNyni, hodinaNyni, minutaNyni, sekundaNyni)

	nazevScriptu = sys.argv[0]

	# Nacist argumenty
	PROG = os.path.basename(os.path.splitext(__file__)[0])
	description = "Stahovani vypisu fio, parsovani csv sporitelny, upload do databaze"
	parser = OptionParser(
		usage='usage: %prog [OPTIONS]',
		version='%s %s' % (PROG, VERSION),
		description=description)
	parser.add_option('-p', '--prvniho',
		action='store_true',
		dest='prvnihoOpt',
		default=False,
		help='odecist prvniho dne v mesici')
	parser.add_option('-a', '--automat',
		action='store_true',
		dest='automatOpt',
		default=False,
		help='Automaticky aktivovat ucty')
	parser.add_option('-v', '--verbose',
		action='store_true',
		dest='verboseOpt',
		default=False,
		help='Ukecana verze, vypisuje i sql commandy')
	parser.add_option('-t', '--test',
		action='store_true',
		dest='testOpt',
		default=False,
		help='Jedeme v testovacim rezimu. Stejne jako ostry, ale bez uploadu do databaze')

	(options, args) = parser.parse_args()

	# uzivatel nezadal ani jeden parametr, len(sys.argv) = 1 a v nem je jen nazev scriptu
	if len(sys.argv) == 1:
		parser.parse_args(['--help'])

	# parametr -p
	if (options.prvnihoOpt):
		if (denNyni == 1):
			OdectiPlatbyPrvniho(con, vysePrispevku, options.testOpt, options.verboseOpt, datetimeTed)
		else:
			print "Dnes neni prvniho, neodecitam"

	# parametr -a
	if (options.automatOpt):
		OdecitatPlatbyAutomaticky(con, vysePrispevku, options.testOpt, options.verboseOpt, datetimeTed)

def OdectiPlatbyPrvniho(con, vysePrispevku, spustitTest, spustitVerbose, datetimeTed):

	# kurzor na databazi
	cur = con.cursor()

	sql = "SELECT id FROM cc"
	cc = dict()
	cur.execute(sql)
	rows = cur.fetchall()
	for row in rows:
		userId = row[0]
		cc[userId] = 1
		# nyni mame v dict cc seznam vsech co maji mit net zdarma

	sql = """SELECT Uzivatel_id, SUM( castka ), money_deaktivace, money_aktivni, TypClenstvi_id
FROM UzivatelskeKonto
JOIN Uzivatel ON Uzivatel.id = UzivatelskeKonto.Uzivatel_id
GROUP BY Uzivatel_id
ORDER BY `UzivatelskeKonto`.`Uzivatel_id` ASC"""

	cur.execute(sql)
	rows = cur.fetchall()
	for row in rows:
		userId = row[0]
		stavKonta = int(row[1])
		stavDeaktivace = row[2]
		stavAktivni = row[3]
		typClenstvi = row[4]

		# Ma uzivatel CC ?
		if cc.has_key(userId):
			maUserCC = True
		else:
			maUserCC = False

		# Kdyz neni radny clen a je neaktivni, tak nas nezajima
		if ((  typClenstvi <= 2 ) and ( stavAktivni == 0)):
			pass

		# Kdyz neni radny clen ale je aktivni, tak ho deaktivujeme, ten nema do netu co delat.
		if (( typClenstvi <= 2 ) and ( stavAktivni == 1)):
			sql = "UPDATE Uzivatel SET money_aktivni = 0"
			sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 3, 0, %s, "[Automat] Deaktivace primarniho uctu", 1""" % (userId, datetimeTed)

		# Je to radny clen, neni deaktivovan, stav aktivni nas nezajima, protoze tady aktivujeme vzdy
		if (( typClenstvi >= 3 ) and ( stavDeaktivace == 0)):
			if not (maUserCC):
				if ( stavKonta >= vysePrispevku):
					# odecti prispevek
					sql = "UPDATE Uzivatel SET money_aktivni = 1"
					sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 4, -290, %s, "[Automat] Clensky prispevek", 1""" % (userId, datetimeTed)

				else:
					# Nema dost penez, deaktivuj ho
					sql = "UPDATE Uzivatel SET money_aktivni = 0"
					sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 3, 0, %s, "[Automat] Deaktivace uctu", 1""" % (userId, datetimeTed)

			# Ma cestne clenstvi
			else:
				sql = "UPDATE Uzivatel SET money_aktivni = 1"
				sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 4, 0, %s, "[Automat] Prodlouzeni CC", 1""" % (userId, datetimeTed)

		# Je to radny clen, je aktivni, je nastaven na deaktivaci
		if (( typClenstvi >= 3 ) and ( stavAktivni == 1) and ( stavDeaktivace == 1)):
			# neodecitej prispevek, deaktivuj
			sql = "UPDATE Uzivatel SET money_aktivni = 0"
			sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 2, 0, %s, "[Automat] Deaktivace uctu", 1""" % (userId, datetimeTed)

		# Je to radny clen, je neaktivni, je nastaven na deaktivaci, tak nas uz nezajima
		if (( typClenstvi >= 3 ) and ( stavAktivni == 0) and ( stavDeaktivace == 1)):
			pass

		try:
			if spustitVerbose: print sql
			if not spustitTest:
				cur.execute(sql)
				con.commit()
		except mdb.Error, e:
			try:
				print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo
			except IndexError:
				print "MySQL Error: %s" % str(e)
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo

		try:
			if spustitVerbose: print sql2
			if not spustitTest:
				cur.execute(sql2)
				con.commit()
		except mdb.Error, e:
			try:
				print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo
			except IndexError:
				print "MySQL Error: %s" % str(e)
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo


def OdecitatPlatbyAutomaticky(con, vysePrispevku, spustitTest, spustitVerbose, datetimeTed):


	if spustitTest: print "spoustim OdecitatPlatbyAutomaticky. Test rezim"
	
	# kurzor na databazi
	cur = con.cursor()

	sql = "SELECT id FROM cc"
	cc = dict()
	cur.execute(sql)
	rows = cur.fetchall()
	for row in rows:
		userId = row[0]
		cc[userId] = 1
		# nyni mame v dict cc seznam vsech co maji mit net zdarma

	sql = """SELECT Uzivatel_id, SUM( castka ), money_deaktivace, money_aktivni, TypClenstvi_id
FROM UzivatelskeKonto
JOIN Uzivatel ON Uzivatel.id = UzivatelskeKonto.Uzivatel_id
GROUP BY Uzivatel_id
ORDER BY `UzivatelskeKonto`.`Uzivatel_id` ASC"""

	cur.execute(sql)
	rows = cur.fetchall()
	for row in rows:
		userId = row[0]
		stavKonta = int(row[1])
		stavDeaktivace = row[2]
		stavAktivni = row[3]
		typClenstvi = row[4]

		# Ma uzivatel CC ?
		if cc.has_key(userId):
			maUserCC = True
		else:
			maUserCC = False

		# Je to radny clen, neni deaktivovan, neni aktivni
		if (( typClenstvi >= 3 ) and ( stavAktivni == 0 ) and (stavDeaktivace == 0)):
			# Nema cestne clenstvi
			if not (maUserCC):
				# Ma dost penez
				if ( stavKonta >= vysePrispevku):
					if spustitTest: print "UID:[%s] Splnuje podminky na aktivaci, ma [%s] penez." % (userId, stavKonta)
					# odecti prispevek
					sql = "UPDATE Uzivatel SET money_aktivni = 1"
					sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 4, -290, %s, "[Automat] Clensky prispevek", 1""" % (userId, datetimeTed)

			# Ma cestne clenstvi
			else:
				sql = "UPDATE Uzivatel SET money_aktivni = 1"
				sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 4, 0, %s, "[Automat] Prodlouzeni CC", 1""" % (userId, datetimeTed)

		try:
			if spustitVerbose: print sql
			if not spustitTest:
				cur.execute(sql)
				con.commit()
		except mdb.Error, e:
			try:
				print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo
			except IndexError:
				print "MySQL Error: %s" % str(e)
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo

		try:
			if spustitVerbose: print sql2
			if not spustitTest:
				cur.execute(sql2)
				con.commit()
		except mdb.Error, e:
			try:
				print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo
			except IndexError:
				print "MySQL Error: %s" % str(e)
				continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo

if __name__ == "__main__":
	main(sys.argv[1:])
