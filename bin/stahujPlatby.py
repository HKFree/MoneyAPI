#! /usr/bin/python
# -*- coding: utf-8 -*-
#
# Author: Kendy 11/2015
# Verze: 1.0
#
# -f stahnuti aktualni platby z FIO API bankovnictvi, zparsovat ji a upload do databaze
# -s stazene csv z csas rozparsuje a uploadne do databaze

import ConfigParser, os, sys, re
from xml.dom import minidom
import MySQLdb as mdb
from datetime import date; from dateutil.relativedelta import *
from optparse import OptionParser

import csv
import shutil
from urllib2 import Request, urlopen, URLError, HTTPError
import urllib
import urllib2

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

	# Nacist argumenty
	PROG = os.path.basename(os.path.splitext(__file__)[0])
	description = "Stahovani vypisu fio, parsovani csv sporitelny, upload do databaze"
	parser = OptionParser(
		usage='usage: %prog [OPTIONS]',
		version='%s %s' % (PROG, VERSION),
		description=description)
	parser.add_option('-f', '--fio',
		action="store",
		type='choice',
		choices=['dnes', 'vcera'],
		dest="datum",
		help="--fio dnes  Stahne vypis za dnesni den            "
			"--fio vcera Stahne vypis za vcerejsi den       "
			"Nasledne ho uploadne do databaze")
	parser.add_option('-z', '--zpracuj',
		action='store_true',
		dest='zpracujOpt',
		default=False,
		help='zpracovat nove platby v databazi')
	parser.add_option('-s', '--sporitelna',
		action='store_true',
		dest='sporitelnaOpt',
		default=False,
		help='Projde adresar s vypisy sporitelny a uploadne vsechny csv do databaze')
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

	# parametr -f
	if ( options.datum ):
		# podle parametru pripravime url
		datumVypisu = ""
		if (re.search(r'(?i)dnes', options.datum)):
			mesicNyni =  "%02d" % int(date.today().month)
			denNyni = "%02d" % int(date.today().day)
			datumVypisu = ( "%s-%s-%s" ) % (date.today().year, mesicNyni, denNyni)
		elif (re.search(r'(?i)vcera', options.datum)):
			oneDayAgo = date.today()+relativedelta(days=-1)
			oneDayAgoDay	= "%02d" % oneDayAgo.day	# vraci nam to format v jednociselnem tvaru, chceme dvoj
			oneDayAgoMonth	= "%02d" % oneDayAgo.month	# vraci nam to format v jednociselnem tvaru, chceme dvoj
			oneDayAgoYear	= oneDayAgo.year
			datumVypisu	= ( "%s-%s-%s" ) % (oneDayAgoYear, oneDayAgoMonth, oneDayAgoDay)

		if (len(datumVypisu) == 0):
			print "prijato nespravne datum v parametru"
			sys.exit(1)

		StahniVypisFio(con, datumVypisu, datumVypisu, fio_url, fio_token, fio_cislo_uctu, options.testOpt, options.verboseOpt)

	# parametr -s
	if (options.sporitelnaOpt):
		UploadVypisCsasDoDb(con, csas_dirvypisynove, csas_dirvypisyzpracovane, options.testOpt, options.verboseOpt)

	# parametr -z
	if (options.zpracujOpt):
		PripisPlatbyNaUzivatelskeKonto(con, options.testOpt, options.verboseOpt)

def PripisPlatbyNaUzivatelskeKonto(con, spustitTest, spustitVerbose):
	cur = con.cursor()

	# Vsechny nove-nezpracovane prichozi platby
	sql = "SELECT id, vs, datum, castka FROM PrichoziPlatba WHERE typ_platby = '1' AND castka > 0"

	cur.execute(sql)
	rows = cur.fetchall()
	for row in rows:
		flags = ""
		idPlatby = row[0]
		vsPlatby = row[1]
		datumPlatby = row[2]
		castkaPlatby = row[3]

		poznamkaPlatby = 'Prichozi platba'
		if ((vsPlatby is None ) or ( int(vsPlatby) == 0 )):			# uzivatel zapomel dat VS
			vsPlatby = "Null"						# Nastavime ho tedy na Null
			TypPohybuNaUctu_id = 10						# Neznama prichozi platba
			poznamkaPlatby = 'Neznama platba, nema VS'
			if spustitVerbose: print "Neznama prichozi platba [%s] - nema VS" % (vsPlatby)
		else:
			sql = "SELECT * FROM Uzivatel WHERE id = %s" % (vsPlatby)
			cur.execute(sql)
			num_rows=cur.rowcount
			if (num_rows == 0):
				if spustitVerbose: print "Nenalezen uzivatel. Platba VS:[%s] Datum:[%s] Castka:[%s]" % (vsPlatby, datumPlatby, castkaPlatby)
				TypPohybuNaUctu_id = 10					# Neznama prichozi platba
				poznamkaPlatby = 'Neznama platba, chybny VS:[%s]' % (vsPlatby)
				vsPlatby = "Null"					# Uzivatel sice dal VS, ale neni v db, ForeignKey by nas nepustil, nastavuji Null
			else:
				TypPohybuNaUctu_id = 1					# Prichozi platba se znamym VS

		# Kontrola na SloucenyUzivatel
		if not (vsPlatby == "Null"):
			sql = "SELECT Uzivatel_id FROM SloucenyUzivatel WHERE slouceny_uzivatel = %s" % (vsPlatby)
			try:
				if spustitVerbose: print sql
				if not spustitTest: 
					cur.execute(sql)
					con.commit()
					rows = cur.fetchall()
					if spustitVerbose: print "vsPlatby pred sloucenim: [%s]" % vsPlatby
					for row in rows:
						poznamkaPlatby = "Prichozi platba na sloucene ID:[%s]" % vsPlatby
						vsPlatby = row[0]
						if spustitVerbose: print "vsPlatby po kontrole na slouceni: [%s]" % vsPlatby
			except mdb.Error, e:
				try:
					print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
					continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo
				except IndexError:
					print "MySQL Error: %s" % str(e)
					continue			# Nechceme oznacit prichozi platbu jako zpracovanou, kdyz sql selhalo

		# Vlozime platbu do UzivatelskeKonto
		sql = """INSERT INTO UzivatelskeKonto (PrichoziPlatba_id, Uzivatel_id, TypPohybuNaUctu_id, castka, datum, poznamka, zmenu_provedl) 
VALUES (%s, %s, %s, %s, '%s', '%s')""" % (idPlatby, vsPlatby, TypPohybuNaUctu_id , castkaPlatby, datumPlatby, poznamkaPlatby, 1)
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

		# A prichozi platbu dle ID zupdatujeme jako zpracovanou
		sql = "UPDATE PrichoziPlatba SET typ_platby = 2 WHERE id = %s" % idPlatby
		try:
			if spustitVerbose: print sql
			if not spustitTest: 
				cur.execute(sql)
				con.commit()
		except mdb.Error, e:
			try:
				print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
			except IndexError:
				print "MySQL Error: %s" % str(e)

def UploadVypisCsasDoDb(con, csas_dirvypisynove, csas_dirvypisyzpracovane, spustitTest, spustitVerbose):

	# slozime vyslednou cestu pro hledani novych vypisu
	csas_dirvypisynove = workdir + "/" + csas_dirvypisynove

	# Vypiseme si jen soubory z cesty a chceme jen soubory s priponou .csv
	files = [ f for f in os.listdir(csas_dirvypisynove) if f.endswith(('.csv')) and os.path.isfile(os.path.join(csas_dirvypisynove,f)) ]
	for file in files:
		f = csas_dirvypisynove + '/' +file
		with open(f, 'rb') as csvfile:
			# rozparsuj csvsoubor
			sr=csv.reader(csvfile, delimiter=';')
			# chceme zpracovat jen data, hlavicka nas nezajima
			csvData = 0
			for row in sr:
				# uz prisly na radu data ?
				if (csvData == 1):
					cur = con.cursor()
					Flags = '1'			# Nova platba
					# VS, SS, Datum, CisloUctu, NazevUctu, Objem, 2010, IDPohybu, ZpravaProPrijemce, Flags, UInfo, Info_Od_Banky, Datum
					Datum = row[0]			# 00 Due date
					Info_Od_Banky = row[1]		# 01 Payment
					CisloUctu = row[2]		# 02 Counter-acc. no.
					Objem = row[3]			# 03 Transaction
					Mena = row[4]			# 04 + 14 Currency
					ZpravaProPrijemce = row[6]	# 06 Info on Payment
					NazevUctu = row[7]		# 07 Counter-acc. name
					VS = row[8]			# 08 Var.symb.1
					IDPohybu = row[9]		# 09 Bank record
					SS = row[11]			# 11 Specific symbol
					Datum = re.sub("/", "-", Datum)
					UInfo = ""			# CSAS neposkytuje
					
					# U objemu je desetinna tecka jako carka. Potrebujem tecku abychom to pak mohli prevest na float cislo
					Objem = re.sub(",", ".", Objem); Objem = float(Objem)
					# Konverze z CP1250 na UTF-8, pac CSAS v dnesni dobe funguje na CP1250 misto standardu UTF-8. V db vsak mame UTF-8
					ZpravaProPrijemce = ZpravaProPrijemce.decode("cp1250"); ZpravaProPrijemce = ZpravaProPrijemce.encode("utf-8")
					NazevUctu = NazevUctu.decode("cp1250"); NazevUctu = NazevUctu.encode("utf-8")
					Info_Od_Banky = Info_Od_Banky.decode("cp1250"); Info_Od_Banky= Info_Od_Banky.encode("utf-8")


					# Aby nebyl porusen unikatni klic, kdyz polozka se rovna 0, tak nastavit polozku na cislo max+1 z rady 99900*
					if (IDPohybu.isdigit()):		# Jedna se o cislo ?
						if (int(IDPohybu) == 0):
							sql = "SELECT MAX(index_platby) FROM PrichoziPlatba WHERE kod_cilove_banky = 0800 AND index_platby like '99900%'"
#							print sql
							try:
								cur.execute(sql)
							except mdb.Error, e:
								try:
									print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
								except IndexError:
									print "MySQL Error: %s" % str(e)
							rows = cur.fetchall()
							for row in rows:
								if (row[0] is None):
									maxIDPohybu = 999000000000	# Zaciname znovu, v praxi by se nemelo stat
								else:
									maxIDPohybu = row[0]
							IDPohybu = int(maxIDPohybu) + 1
					if (Objem < 0.0):
						# Bankovni poplatek
						if (re.search(r'(?i)poplatek', Info_Od_Banky)):
							Flags = "4"
					else:
						# Typ teto platby nezname
						Flags = "10"

					# Mame vsechno, stvor SQL a posli do db
					sql = """INSERT INTO PrichoziPlatba (vs, ss, datum, cislo_uctu, nazev_uctu, castka, kod_cilove_banky, index_platby, 
qzprava_prijemci, typ_platby, identifikace_uzivatele, info_od_banky) 
VALUES( %s, %s, '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')
ON DUPLICATE KEY UPDATE datum = '%s'""" % (VS, SS, Datum, CisloUctu, NazevUctu, Objem, 800, IDPohybu, ZpravaProPrijemce, Flags, UInfo, Info_Od_Banky, Datum )

					try:
						if spustitVerbose == True:
							print sql
							print
						if spustitTest == False:
							cur.execute(sql)
							con.commit()
					except mdb.Error, e:
						try:
							print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
						except IndexError:
							print "MySQL Error: %s" % str(e)

				# uz jsme dosli k hlavicce v CSV souboru? Tak nastav ze v pristim cyklu jsou data
				if ( (len(row) == 21) and (csvData == 0)):
					csvData = 1
			if (csvData == 0):
				print "Nevalidni CSV soubor, ocekavano 21 poli pro radky s vypisy"
				sys.exit(1)

		# Nazev ciloveho souboru. Metoda shutil.move neni jako mv v linuxu, kde se dava cilovy adresar, zde je treba specifikovat presnou cestu vcetne nazvu ciloveho souboru
		kam_presunout = workdir + "/" + csas_dirvypisyzpracovane + "/" + file
		shutil.move (f, kam_presunout)
# ZPRACOVAT VYPISY CSAS KONEC

# Stahnout Vypis FIO za urcity casovy usek (rozliseni je v radech dnu). Nasledne uploadnout do databaze
def StahniVypisFio(con, datumOd, datumDo, fio_url, fio_token, fio_cislo_uctu, spustitTest, spustitVerbose):

	# kurzor na databazi
	cur = con.cursor()

	# Upravit URL na stazeni vypisu
	fio_url = re.sub("DATUMOD", datumOd, fio_url)
	fio_url = re.sub("DATUMDO", datumDo, fio_url)
	orig_fio_url = fio_url				# jen pro error vypis, at v logu nemame token
	fio_url = re.sub("TOKEN", fio_token, fio_url)

	# stahnout z webu xml vypis
	try:
		response = urllib2.urlopen(fio_url)
		code = response.getcode()
	except URLError as e:
		if hasattr(e, 'reason'):
			print 'We failed to reach a server.'
			print 'Reason: ', e.reason
			sys.exit(1)
		elif hasattr(e, 'code'):
			print 'Error code: ', e.code
			print "HTTP get [%s] nemel status kod 200, ale %s. Koncim" % (orig_fio_url, e.code)
			sys.exit(1)
		else:
			print "Unknown error on HTTP get"
			sys.exit(1)
	xmldataHTTP = response.read()

	# rozparsujeme vysledne XML
#	xml = minidom.parse('transakce.xml')
	xml = minidom.parseString(xmldataHTTP)

	# Vytahnem z vypisu cislo uctu
	info = xml.getElementsByTagName("Info")
	for i in info:
		# TODO chceme zpracovavat vypis s nasim cislem uctu, takze dodelat kontrolu
		cislo_uctu = i.getElementsByTagName('accountId')[0].childNodes[0].data
		if not (cislo_uctu == fio_cislo_uctu):
			print "Nesouhlasi cislo uctu, nepokracuji v cinnosti."
			sys.exit(1)

	# cislo uctu je tedy spravne, projdem jednotlive transakce
	transactions = xml.getElementsByTagName('Transaction')
	for t in transactions:
		# Neni pravidlo ze ma stahle XML vsechny polozky, tak osetrime jejich pripadnou neexistenci

		# povinne polozky, ty tu musi byt vzdy (http://www.fio.cz/xsd/IBSchema.xsd)
		try:	IDPohybu = t.getElementsByTagName('column_22')[0].childNodes[0].data	# ID_Pohybu
		except Exception,e:
			# tato hlaska pak prijde mailem od cronu
			print "IDpohybu neni v polozkach, to je vsak povinna polozka, predpoklad je tedy, ze jsme stahli necely vypis. Preskakuji."
			continue

		try:	Datum    = t.getElementsByTagName('column_0')[0].childNodes[0].data	# Datum pohybu
		except Exception,e:
			# tato hlaska pak prijde mailem od cronu
			print "Datum neni v polozkach, to je vsak povinna polozka, predpoklad je tedy, ze jsme stahli necely vypis. Preskakuji."
			continue
		# Datum nam predavaji ve formatu 2015-11-10+01:00, potrebujem vsak jen YYYY-MM-DD
		Datum = Datum[:10]

		try:	Objem    = t.getElementsByTagName('column_1')[0].childNodes[0].data	# Suma v penezich
		except Exception,e:
			# tato hlaska pak prijde mailem od cronu
			print "Objem neni v polozkach, to je vsak povinna polozka, predpoklad je tedy, ze jsme stahli necely vypis. Preskakuji."
			continue

		try:	Mena = t.getElementsByTagName('column_14')[0].childNodes[0].data	# Mena prichozi platby
		except Exception,e:
			# tato hlaska pak prijde mailem od cronu
			print "Mena neni v polozkach, to je vsak povinna polozka, predpoklad je tedy, ze jsme stahli necely vypis. Preskakuji."
			continue

		# Volitelne polozky
		try:	Ucet     = t.getElementsByTagName('column_2')[0].childNodes[0].data	# Cislo protiuctu
		except Exception,e:	Ucet = "0"

		try:	KodBanky = t.getElementsByTagName('column_3')[0].childNodes[0].data	# Kod banky protiuctu
		except Exception,e:	KodBanky = "0"

		try:	VS       = t.getElementsByTagName('column_5')[0].childNodes[0].data	# Variabilni Symbol
		except Exception,e:	VS = "null"

		try:	SS       = t.getElementsByTagName('column_6')[0].childNodes[0].data	# Specificky Symbol
		except Exception,e:	SS = "null"

		try:	UInfo    = t.getElementsByTagName('column_7')[0].childNodes[0].data	# Uzivatelska identifikace
		except Exception,e:	UInfo = ""

		try:	Typ      = t.getElementsByTagName('column_8')[0].childNodes[0].data	# Typ operace
		except Exception,e:	Typ = ""

		try:	Provedl  = t.getElementsByTagName('column_9')[0].childNodes[0].data	# Jmeno Uctu
		except Exception,e:	Provedl = ""

		try:	NazevUctu = t.getElementsByTagName('column_10')[0].childNodes[0].data	# Nazev Protiuctu
		except Exception,e:	NazevUctu = ""

		try:	ZpravaProPrijemce = t.getElementsByTagName('column_16')[0].childNodes[0].data	# Zprava pro prijemce
		except Exception,e:	ZpravaProPrijemce = ""

		try:	BankaInfo = t.getElementsByTagName('column_18')[0].childNodes[0].data	# Banka Info
		except Exception,e:	BankaInfo = ""

		try:	Komentar  = t.getElementsByTagName('column_25')[0].childNodes[0].data	# Komentar
		except Exception,e:	Komentar = ""

		# Cislo uctu si drzime ve formatu UCETCISLO/KODBANKY
		CisloUctu = Ucet + "/" + KodBanky

		# Tyto informace nam staci v jedne kolonce (vic jich v modelu neni :)) takze je sloucime. Pridame pripadne mezery
		Info_Od_Banky = ""
		if (len(BankaInfo) > 0):
			Info_Od_Banky = BankaInfo
			BankaInfoSeparator = " "
		else:
			BankaInfoSeparator = ""
		if (len(Komentar) > 0):
			Info_Od_Banky = Info_Od_Banky + BankaInfoSeparator + Komentar
			KomentarSeparator = " "
		else:
			KomentarSeparator = BankaInfoSeparator
		if (len(Typ) >0):
			Info_Od_Banky = Info_Od_Banky + KomentarSeparator + Typ

		# Sem tam nam useri poslou v textu jednoduchou ci dvojitou uvozovku, aby nedoslo problemum s vkladanim do db, tak to escapneme
		ZpravaProPrijemce = re.sub("'", "\\'", ZpravaProPrijemce)
		Info_Od_Banky = re.sub("'", "\\'", Info_Od_Banky)
		ZpravaProPrijemce = re.sub("\"", "\\\"", ZpravaProPrijemce)
		Info_Od_Banky = re.sub("\"", "\\\"", Info_Od_Banky)

		# Ve vychozim stavu maji uplne vsechny platby flag 1, nejake zname pripady muzeme rovnou upravit na jine
		Flags = '1'

		# Jelikoz nepracujeme s kurzovnim listkem, tak automaticky oznacime jako neznamou platbu, ktera dosla v jine mene.
		if not (re.search("CZK", Mena)):
			Flags = '13'
			print "Dosla prichozi platba v jine mene"

		if (float(Objem) < 0.0):
			# Bankovni poplatek
			if (re.search(r'(?i)poplatek', Typ)):
				Flags = "4"
			# Vyber pokladnou
			elif (re.search(r'poklad', Typ)):
				Flags = "11"
				if (len(Info_Od_Banky)>0):
					Info_Od_Banky = Info_Od_Banky + " " + Provedl
				else:
					Info_Od_Banky = Provedl
			else:
				# Typ teto platby nezname
				Flags = "10"

		sql = """INSERT INTO PrichoziPlatba (vs, ss, datum, cislo_uctu, nazev_uctu, castka, kod_cilove_banky, index_platby, 
zprava_prijemci, typ_platby, identifikace_uzivatele, info_od_banky) 
VALUES( %s, %s, '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')
ON DUPLICATE KEY UPDATE datum = '%s'""" % (VS, SS, Datum, CisloUctu, NazevUctu, Objem, 2010, IDPohybu, ZpravaProPrijemce, Flags, UInfo, Info_Od_Banky, Datum )

		try:
			if spustitVerbose == True:
				print sql
				print
			cur.execute(sql)
			con.commit()
		except mdb.Error, e:
			try:
				print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
			except IndexError:
				print "MySQL Error: %s" % str(e)
# ZPRACOVAT VYPISY FIO KONEC

if __name__ == "__main__":
	main(sys.argv[1:])
