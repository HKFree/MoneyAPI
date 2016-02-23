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
import syslog

VERSION = '1.0.0'

# Aktualni pracovni adresar scriptu, s nim si automaticky nactem konfigurak
workdir = os.path.dirname(os.path.realpath(__file__))
config_file = workdir + '/../etc/money.ini'

# spolecne funkce pro money
sys.path.insert(0, workdir + '/../lib')
from moneyLib import *

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

	# Otevrit syslog a nastavit nazev na nazev scriptu
	syslog.openlog(nazevScriptu)

	# uzivatel nezadal ani jeden parametr, len(sys.argv) = 1 a v nem je jen nazev scriptu
	if len(sys.argv) == 1:
		parser.parse_args(['--help'])

	# parametr -p
	if (options.prvnihoOpt):
		if (int(denNyni) == 1):
			OdectiPlatbyPrvniho(con, vysePrispevku, options.testOpt, options.verboseOpt, datetimeTed)
		else:
			print "Dnes [%s] neni prvniho, neodecitam" % (datetimeTed)

	# parametr -a
	if (options.automatOpt):
		OdecitatPlatbyAutomaticky(con, vysePrispevku, options.testOpt, options.verboseOpt, datetimeTed)

def OdectiPlatbyPrvniho(con, vysePrispevku, testOpt, verboseOpt, datetimeTed):

	# V databazi je format date, mysql nam pise warningy, coz nechcem, takze pokud se zmeni format, smazat tuto radku
	datetimeTed = datetimeTed[0:10]

	sql = "SELECT id FROM cc"
	cc = dict()
	rows, numRows = spustSql(con, sql, False, verboseOpt)

	for row in rows:
		userId = row[0]
		cc[userId] = 1
		# nyni mame v dict cc seznam vsech co maji mit net zdarma

	# Vsechny ucty z databaze, vyjma userId == 1, to je specialni uzivatel, toho nebudem procesovat
	sql = """SELECT Uzivatel.id, SUM( castka ), money_deaktivace, money_aktivni, TypClenstvi_id
FROM Uzivatel
LEFT OUTER JOIN UzivatelskeKonto ON Uzivatel.id = UzivatelskeKonto.Uzivatel_id
WHERE Uzivatel.id > 1
GROUP BY Uzivatel.id
ORDER BY `Uzivatel`.`id` ASC"""
	rows, numRows = spustSql(con, sql, False, verboseOpt)
	del sql

	for row in rows:
		sql = ""; sql2 = ""
		# db nam vraci vse ve stringu, potrebujeme int
		userId = int(row[0])
		stavKonta = int(row[1])
		stavDeaktivace = int(row[2])
		stavAktivni = int(row[3])
		typClenstvi = int(row[4])

		# Ma uzivatel CC ?
		if cc.has_key(userId):
			maUserCC = True
		else:
			maUserCC = False

		# Neni to radny clen
		if ( typClenstvi <= 2 ):
			# Je neaktivni, tak nas nezajima a jdem na dalsiho usera
			if (stavAktivni == 0):
				continue
			else:
				# Kdyz to neni radny clen, ale je aktivni, tak ho deaktivujeme, ten nema do netu co delat
				sql = "UPDATE Uzivatel SET money_aktivni = 0 WHERE id = %s" % (userId)
				sql2 = """INSERT INTO UzivatelskeKonto 
	(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
	(null, %s, 3, 0, "%s", "[Automat] Deaktivace primarniho uctu", 1)""" % (userId, datetimeTed)
				syslog.syslog("Deaktivace [nespravne clenstvi] userId:[%s] typClenstvi:[%s] stavAktivni:[%s] stavDeaktivace:[%s] maUserCC:[%s] stavKonta:[%s]" % ( userId, typClenstvi, stavAktivni, stavDeaktivace, maUserCC, stavKonta))

		# Je to radny clen
		if ( typClenstvi >= 3 ):
			# neni deaktivovan, stav aktivni nas nezajima, protoze tady aktivujeme vzdy (vyjma Cestnych Clenu)
			if (stavDeaktivace == 0):
				# Nema cestne clenstvi
				if not (maUserCC):
					if ( stavKonta >= vysePrispevku):
						# Odecti prispevek
						sql = "UPDATE Uzivatel SET money_aktivni = 1 WHERE id = %s" % (userId)
						sql2 = """INSERT INTO UzivatelskeKonto 
	(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
	(null, %s, 4, -%i, "%s", "[Automat] Clensky prispevek", 1)""" % (userId, vysePrispevku, datetimeTed)
						syslog.syslog("Odecitam prispevek userId:[%s] typClenstvi:[%s] stavAktivni:[%s] stavDeaktivace:[%s] maUserCC:[%s] stavKonta:[%s]" % ( userId, typClenstvi, stavAktivni, stavDeaktivace, maUserCC, stavKonta))

					else:
						if (stavAktivni == 0):
							# Neaktivni co nema penize = nevsimam si ho
							continue
						else:
							# Nema dost penez, deaktivuj ho
							sql = "UPDATE Uzivatel SET money_aktivni = 0 WHERE id = %s" % (userId)
							sql2 = """INSERT INTO UzivatelskeKonto 
		(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
		(null, %s, 3, 0, "%s", "[Automat] Deaktivace uctu", 1)""" % (userId, datetimeTed)
							syslog.syslog("Deaktivace [malo penez] userId:[%s] typClenstvi:[%s] stavAktivni:[%s] stavDeaktivace:[%s] maUserCC:[%s] stavKonta:[%s]" % ( userId, typClenstvi, stavAktivni, stavDeaktivace, maUserCC, stavKonta))

				# Ma cestne clenstvi
				else:
					if (stavAktivni == 1):
						textSql2 = "[Automat] Prodlouzeni CC"
					else:
						textSql2 = "[Automat] Aktivuji CC"
					sql = "UPDATE Uzivatel SET money_aktivni = 1 WHERE id = %s" % (userId)
					sql2 = """INSERT INTO UzivatelskeKonto 
	(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
	(null, %s, 4, 0, "%s", "%s", 1)""" % (userId, datetimeTed, textSql2)
					syslog.syslog("CC, [%s] userId:[%s] typClenstvi:[%s] stavAktivni:[%s] stavDeaktivace:[%s] maUserCC:[%s] stavKonta:[%s]" % ( textSql2, userId, typClenstvi, stavAktivni, stavDeaktivace, maUserCC, stavKonta))

			# Je to radny clen a je nastaven na deaktivaci
			else:
				if ( stavAktivni == 1):
					# Jestli je aktivni, tak neodecitej prispevek, deaktivuj ho
					sql = "UPDATE Uzivatel SET money_aktivni = 0 WHERE id = %s" % (userId)
					sql2 = """INSERT INTO UzivatelskeKonto 
		(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
		(null, %s, 2, 0, "%s", "[Automat] Deaktivace uctu", 1)""" % (userId, datetimeTed)
					syslog.syslog("Deaktivace [urcen k deaktivaci] userId:[%s] typClenstvi:[%s] stavAktivni:[%s] stavDeaktivace:[%s] maUserCC:[%s] stavKonta:[%s]" % ( userId, typClenstvi, stavAktivni, stavDeaktivace, maUserCC, stavKonta))
				else:
					# Je to radny clen, je neaktivni, je nastaven na deaktivaci, tak nas uz nezajima
					continue

		if (len(sql) == 0):
			# Zde pro jistotu, kdyby nam neco uteklo...
			print "User s ID:[%s] typClenstvi:[%s] stavAktivni:[%s] stavDeaktivace:[%s] maUserCC:[%s] stavKonta:[%s] nebyl zprocesovan" % ( userId, typClenstvi, stavAktivni, stavDeaktivace, maUserCC, stavKonta)
			continue

		spustSql(con, sql, testOpt, verboseOpt)
		spustSql(con, sql2, testOpt, verboseOpt)

def OdecitatPlatbyAutomaticky(con, vysePrispevku, testOpt, verboseOpt, datetimeTed):
	if testOpt: print "spoustim OdecitatPlatbyAutomaticky. Test rezim"

	denDnes = int(datetimeTed[8:10])

	# V databazi je format date, mysql nam pise warningy, coz nechcem, takze pokud se zmeni format, smazat tuto radku
	datetimeTed = datetimeTed[0:10]

	sql = "SELECT id FROM cc"
	cc = dict()
	rows, numRows = spustSql(con, sql, False, verboseOpt)
	for row in rows:
		userId = row[0]
		cc[userId] = 1
		# nyni mame v dict cc seznam vsech co maji mit net zdarma

	sql = """SELECT Uzivatel_id AS userId, SUM( castka ) AS stavKonta, money_automaticka_aktivace_do AS aktivovatDo
FROM UzivatelskeKonto AS UK
JOIN Uzivatel AS U ON U.id = UK.Uzivatel_id
WHERE U.id >1 AND U.TypClenstvi_id >= 3 AND U.money_aktivni = 0 AND U.money_deaktivace = 0
GROUP BY userId
ORDER BY UK.Uzivatel_id ASC"""

	rows, numRows = spustSql(con, sql, False, verboseOpt)
	for row in rows:
		sql = ""
		sql2 = ""
		userId = int(row[0])
		stavKonta = int(row[1])
		aktivovatDo = int(row[2])

		# Ma uzivatel CC ?
		if cc.has_key(userId):
			maUserCC = True
		else:
			maUserCC = False

		# Nema cestne clenstvi
		if not (maUserCC):
			# Ma dost penez
			if ( stavKonta >= vysePrispevku):
				if testOpt: print "UID:[%s] Splnuje podminky na aktivaci, ma [%s] penez." % (userId, stavKonta)

				if (denDnes <= aktivovatDo):
					# odecti prispevek
					sql = "UPDATE Uzivatel SET money_aktivni = 1 WHERE id = %s" % (userId)
					sql2 = """INSERT INTO UzivatelskeKonto 
(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
(null, %s, 4, -%i, "%s", "[Automat] Clensky prispevek", 1)""" % (userId, vysePrispevku, datetimeTed)
					syslog.syslog("Odecitam prispevek userId:[%s] predchozi stavKonta:[%s], nema CC" % ( userId, stavKonta))

		# Tento blok je jen priprava, v pripade ze budem strojove aktivovat usery s CC -> jejich clenstvi, tak prave timto blokem
		#  zatim tedy nepouzito
		# Ma cestne clenstvi
#		else:
#			textSql2 = "[Automat] Aktivuji CC"
#			sql = "UPDATE Uzivatel SET money_aktivni = 1 WHERE id = %s" % (userId)
#			sql2 = """INSERT INTO UzivatelskeKonto 
#(PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) VALUES 
#(null, %s, 4, 0, "%s", "%s", 1)""" % (userId, datetimeTed, textSql2)
#			syslog.syslog("CC, [%s] userId:[%s]" % ( textSql2, userId ))

		if (len(sql) == 0):
			# Nic ke spusteni, jdem na dalsiho usera
			continue

		spustSql(con, sql, testOpt, verboseOpt)
		spustSql(con, sql2, testOpt, verboseOpt)

if __name__ == "__main__":
	main(sys.argv[1:])
