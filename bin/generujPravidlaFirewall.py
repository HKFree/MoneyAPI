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
firewall_dir = workdir + '/../firewall-pravidla'
# Nacist ini
if os.path.exists(config_file):
	try:
		config = ConfigParser.RawConfigParser()
		config.read(config_file)
		mysql_dbhost	= config.get('mysql', 'dbhost')
		mysql_dbname	= config.get('mysql', 'dbname')
		mysql_dbuser	= config.get('mysql', 'dbuser')
		mysql_dbpass	= config.get('mysql', 'dbpass')
		fio_token	= config.get('fio'  , 'token')
		fio_url		= config.get('fio'  , 'url')
		fio_cislo_uctu	= config.get('fio'  , 'cislo_uctu')
		csas_dirvypisynove		= config.get('csas'  , 'dirvypisynove')
		csas_dirvypisyzpracovane	= config.get('csas'  , 'dirvypisyzpracovane')
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

	nazevScriptu = sys.argv[0]

	# Aktualni cas
	mesicNyni	= "%02d" % int(datetime.today().month)
	denNyni		= "%02d" % int(datetime.today().day)
	hodinaNyni	= "%02d" % int(datetime.today().hour)
	minutaNyni	= "%02d" % int(datetime.today().minute)
	sekundaNyni	= "%02d" % int(datetime.today().second)
	datetimeTed = ( "%s-%s-%s %s:%s:%s" ) % (datetime.today().year, mesicNyni, denNyni, hodinaNyni, minutaNyni, sekundaNyni)

	# Nacist argumenty
	PROG = os.path.basename(os.path.splitext(__file__)[0])
	description = "Stahovani vypisu fio, parsovani csv sporitelny, upload do databaze"
	parser = OptionParser(
		usage='usage: %prog [OPTIONS]',
		version='%s %s' % (PROG, VERSION),
		description=description)
	parser.add_option('-p', '--ipset',
		action='store_true',
		dest='ipsetOpt',
		default=False,
		help='Vygeneruje aktualni IPsety')
	parser.add_option('-s', '--smtp',
		action='store_true',
		dest='smtpOpt',
		default=False,
		help='Vygeneruje aktualni pravidla pro povolene SMTP servery')
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
	if (options.ipsetOpt):
		generujIPsety(con, firewall_dir, options.testOpt, options.verboseOpt, nazevScriptu, datetimeTed)

	if (options.smtpOpt):
		generujSMTPomezeni(con, firewall_dir, options.testOpt, options.verboseOpt, nazevScriptu, datetimeTed)



def generujIPsety(con, firewall_dir, testOpt, verboseOpt, nazevScriptu, datetimeTed):

	# Nazvy souboru ipsetu
	file10107 = firewall_dir + '/ipset10107.cfg'
	file89248 = firewall_dir + '/ipset89248.cfg'
	# kurzor na databazi
	cur = con.cursor()

	# Z db chceme vsechny IPcka co jsou povoleny do netu a useri jsou aktivni
	sql = "select IPAdresa.ip_adresa FROM IPAdresa, Uzivatel WHERE IPAdresa.internet = 1 AND IPAdresa.Uzivatel_id = Uzivatel.id AND Uzivatel.money_aktivni = 1"
	try:
		if verboseOpt == True:
			print "Poustim sql dotaz [%s] nad databazi pro ziskani seznamu IP" % sql
			print
		if testOpt == False:
			cur.execute(sql)
			rows = cur.fetchall()
	except mdb.Error, e:
		try:
			print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
		except IndexError:
			print "MySQL Error: %s" % str(e)

	# Otevrit soubor pro zapis IPSetu
	try:
		if testOpt == False:
			f10107 = open(file10107,'w')
		if verboseOpt == True:
			print "Oteviram soubor [%s] pro zapis" % file10107
	except IOError:
		print "Nemohu otevrit %s pro zapis" % (file10107)
		sys.exit(1)

	# Otevrit soubor pro zapis IPSetu
	try:
		if testOpt == False:
			f89248 = open(file89248,'w')
		if verboseOpt == True:
			print "Oteviram soubor [%s] pro zapis" % file89248
	except IOError:
		print "Nemohu otevrit %s pro zapis" % (file89248)
		sys.exit(1)

	# Zapis hlavicky do souboru
	if testOpt == False:
		f10107.write('# Generovano z money HKfree v %s (%s)\n' % (datetimeTed, nazevScriptu) )
		f89248.write('# Generovano z money HKfree v %s (%s)\n' % (datetimeTed, nazevScriptu) )
		f10107.write('create temporaryIPSET10107-HKfree bitmap:ip range 10.107.0.0-10.107.255.255\n')
		f89248.write('create temporaryIPSET89248-HKfree bitmap:ip range 89.248.240.0-89.248.255.255\n')

	# Projit vysledky z databaze a zapsat do souboru ipsety
	for row in rows:
		ipAdresa = row[0]
		if re.search("10.107", ipAdresa):
			if testOpt == False:
				f10107.write('add temporaryIPSET10107-HKfree %s\n' % (ipAdresa))
		elif re.search("89.248", ipAdresa):
			if testOpt == False:
				f89248.write('add temporaryIPSET89248-HKfree %s\n' % (ipAdresa))
		else:
			print "Nevalidni ip Adresa [%s]" % (ipAdresa)

	# uzavrit soubory
	if testOpt == False:
		f10107.close()
		f89248.close()

def generujSMTPomezeni (con, firewall_dir, testOpt, verboseOpt, nazevScriptu, datetimeTed):

	# Nazvy souboru ipsetu
	fileSMTP = firewall_dir + '/SMTP'

	# kurzor na databazi
	cur = con.cursor()

	# seznam SMTP serveru freemailu
	freeSmtpServery = [ 'smtp.centrum.cz', 
		'smtp.seznam.cz',
		'smtp.volny.cz',
		'smtp.iol.cz',
		'smtp.o2isp.cz',
		'mail2.webstep.net',
		'smtp.servery.cz',
		'smtp.gmail.com',
		'mail.stable.cz',
		'smtp.tiscali.cz',
		'smtp.sloane.cz',
		'mail.cernahora.cz',
		'out.smtp.cz',
		'smtp.posys.cz',
		'mail.inethosting.cz',
		'mail2.pipni.cz ',
		'smtp.cesky-hosting.cz',
		]

	# Otevrit soubor pro zapis pravidel SMTP
	try:
		if testOpt == False:
			fSMTP = open(fileSMTP,'w')
		if verboseOpt == True:
			print "Oteviram soubor [%s] pro zapis" % fileSMTP
	except IOError:
		print "Nemohu otevrit %s pro zapis" % (fileSMTP)
		sys.exit(1)

	sql = "SELECT ip_adresa, TypPovolenehoSMTP_id FROM `PovoleneSMTP` JOIN IPAdresa ON IPAdresa.id = PovoleneSMTP.IPAdresa_id WHERE internet =1"

	try:
		if verboseOpt == True:
			print "Poustim sql dotaz [%s] nad databazi pro ziskani seznamu IP" % sql
			print
		if testOpt == False:
			cur.execute(sql)
			rows = cur.fetchall()
	except mdb.Error, e:
		try:
			print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
		except IndexError:
			print "MySQL Error: %s" % str(e)

	# Zapsat hlavicku pro STMP pravidla
	fSMTP.write('# Generovano z money HKfree v %s (%s)\n\n' % (datetimeTed, nazevScriptu) )
	fSMTP.write(':SMTP - [0:0]\n')

	# Projit vysledky z databaze a zapsat do souboru se SMTP pravidly
	for row in rows:
		ipAdresa = row[0]
		typ = row[1]
		if ((typ == 1) or (typ == 3)):
			fSMTP.write('-A SMTP -s %s -j ACCEPT\n' % ipAdresa)
		if ((typ == 2) or (typ == 3)):
			fSMTP.write('-A SMTP -d %s -j ACCEPT\n' % ipAdresa)


	fSMTP.write('\n# Freemaily jejich SMTP\n\n')
	for ipAdresa in freeSmtpServery:
		fSMTP.write('-A SMTP -d %s -j ACCEPT\n' % ipAdresa)

	# Zapsat finalni cast pro STMP pravidla
	fSMTP.write('\n\n-A SMTP -m limit --limit 5/min --limit-burst 7 -j LOG --log-prefix "** SMTP LOG DROP **"\n')
	fSMTP.write('-A SMTP -j REJECT --reject-with icmp-port-unreachable\n')
	fSMTP.close()


if __name__ == "__main__":
	main(sys.argv[1:])
