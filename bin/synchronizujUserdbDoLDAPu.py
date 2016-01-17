#! /usr/bin/python
# -*- coding: utf-8 -*-
#
# Author: Kendy 01/2016
#

import ConfigParser, os, sys, re
import MySQLdb as mdb
from optparse import OptionParser
import ldap
import ldap.modlist as modlist
import time
from datetime import datetime

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
		ldap_admindn	= config.get('ldap', 'admindn')
		ldap_adminpwd	= config.get('ldap', 'adminpwd')
		ldap_server	= config.get('ldap', 'server')
		ldap_baseDn	= config.get('ldap', 'basedn')
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
	sql = "SET NAMES utf8"
	cur = con.cursor()
	cur.execute(sql)
	con.commit()


	nazevScriptu = sys.argv[0]

	# Nacist argumenty
	PROG = os.path.basename(os.path.splitext(__file__)[0])
	description = "Synchronizator databaze -> LDAP. Volba -m pouze najde v LDAPu uzivatele co jsou navic a ty co tam chybi. Volba -p syncuje uplne vsechny atributy + to co dela -m."
	parser = OptionParser(
		usage='usage: %prog [OPTIONS]',
		version='%s %s' % (PROG, VERSION),
		description=description)
	parser.add_option('-u', '--useri',
		action='store_true',
		dest='useriOpt',
		default=False,
		help='Spustit synchronizaci uzivatelu')
	parser.add_option('-p', '--plna',
		action='store_true',
		dest='plnaOpt',
		default=False,
		help='Synchronizace uzivatelu plna TBD')
	parser.add_option('-m', '--min',
		action='store_true',
		dest='minimalistickaOpt',
		default=False,
		help='Synchronizace uzivatelu minimalisticka (vymaz, pridani) TBD')
	parser.add_option('-r', '--role',
		action='store_true',
		dest='roleOpt',
		default=False,
		help='Spustit synchronizaci roli')
	parser.add_option('-v', '--verbose',
		action='store_true',
		dest='verboseOpt',
		default=False,
		help='Ukecana verze')
	parser.add_option('-t', '--test',
		action='store_true',
		dest='testOpt',
		default=False,
		help='Jedeme v testovacim rezimu. Stejne jako ostry, ale bez uploadu do LDAPu')

	(options, args) = parser.parse_args()

	# uzivatel nezadal ani jeden parametr, len(sys.argv) = 1 a v nem je jen nazev scriptu
	if len(sys.argv) == 1:
		parser.parse_args(['--help'])

	# vytvorit spojeni s LDAPem
	ldapCon = ldap.initialize(ldap_server)
	try:
		ldapCon.protocol_version = ldap.VERSION3
		ldapCon.simple_bind_s(ldap_admindn, ldap_adminpwd)
		valid = True
	except Exception, error:
		print error

	if (options.useriOpt):
		syncLDAPuseri (con, options.testOpt, options.verboseOpt, ldapCon, ldap_baseDn)

	if (options.roleOpt):
		syncLDAProle (con, options.testOpt, options.verboseOpt, ldapCon, ldap_baseDn)


def syncLDAPuseri (con, testOpt, verboseOpt, ldapcon, ldap_baseDn):
	sql = "SELECT id, nick, email, jmeno, prijmeni, heslo FROM Uzivatel WHERE TypClenstvi_id >1 ORDER BY Uzivatel.id ASC"
	rows, numRows = spustSql(con, sql, testOpt, verboseOpt)

	# Naplnime useriDb{} podle vysledku z db, to pak budem porovnavat
	useriDb = {}
	for row in rows:
		user = {}
		uid = int(row[0])
		user['nick'] = row[1]
		user['email'] = row[2]
		user['jmeno'] = row[3]
		user['prijmeni'] = row[4]
		user['heslo'] = row[5]
		useriDb[uid] = user

	# Vytahnem z LDAPu vsechny usery do ldapUseri{}
	ldapFilter = "(&(objectClass=inetOrgPerson))"
	ldap_attributes = ["*"]
	ldapUseri = {}
	try:
		result_id = ldapcon.search(ldap_baseDn, ldap.SCOPE_SUBTREE, ldapFilter, ldap_attributes)
		while 1:
			try:
				result_type, result_data = ldapcon.result(result_id, 0)
			except ldap.NO_SUCH_OBJECT:
				raise DirectoryError("Distinguished name (%s) does not exist." % ldap_baseDn)
			if (result_data == []):
				break
			else:
				if result_type == ldap.RES_SEARCH_ENTRY:
					res = dict()
					dn = result_data[0][0]
					data = result_data[0][1]
					uid = int(data['uid'][0])
					res['dn'] = dn
					res['data'] = data
					# cele ulozit do velkeho hashe
					ldapUseri[uid] = res
	except ldap.LDAPError, e:
		print e

	# nyni porovname seznamy UIDcek a podle vysledku bud do LDAPu pridame zaznam nebo ho uberem

	dbUIDs = useriDb.keys()
	ldapUIDs = ldapUseri.keys()

	# Pro ty co nejsou v LDAPU, tak ty tam pridame
	for dbUID in dbUIDs:
		if not dbUID in ldapUIDs:
			if not ((dbUID == 1) or (dbUID == 555)):	# UID 1 je specialni user, do LDAPu nema pristupy
				print "UID z Db[%s] neni v LDAPu, pridam ho." % (dbUID)
				user = useriDb[dbUID]

				# atributy noveho dn
				# pozor ldap nema rad unicode, tak stringy nutno prekonvertovat do utf-8
				dn = "uid=%s,ou=People,dc=hkfree,dc=org" % (dbUID)
				attrs = {}
				attrs['employeeNumber'] = str(dbUID)
				attrs['givenName'] = user['nick'].encode("utf-8")
				attrs['mail'] = user['email'].encode("utf-8")
				attrs['displayName'] = user['nick'].encode("utf-8")
				attrs['objectClass'] = ['inetOrgPerson','uidObject']
				attrs['uid'] = str(dbUID)
				attrs['cn'] = str(dbUID)
				attrs['sn'] = "%s %s" % (user['jmeno'].encode("utf-8"), user['prijmeni'].encode("utf-8"))
				attrs['userPassword'] = user['heslo'].encode("utf-8")

				# pridat noveho usera
				try:
					result = ldapcon.add(dn, ldap.modlist.addModlist(attrs))
				except ldap.TYPE_OR_VALUE_EXISTS:
					print "Error LDAP: TYPE_OR_VALUE_EXISTS [%s]" % (dn)
					pass
				except ldap.CONSTRAINT_VIOLATION:
					print "Error LDAP ldap.ldap.CONSTRAINT_VIOLATION"
					pass
				except ldap.SERVER_DOWN:
					print "Error: asi spadl LDAP server"
					sys.exit(1)

	# Pro ty co jsou v LDAPU, ale ne v db, tak ty smazeme
	for ldapUID in ldapUIDs:
		if not ldapUID in dbUIDs:
			print "UID z LDAPu[%s] neni v db, smazu ho." % (ldapUID)
			dn = "uid=%s,ou=People,dc=hkfree,dc=org" % (ldapUID)

			# smazat usera
			try:
				result = ldapcon.delete_s(dn)
			except ldap.CONSTRAINT_VIOLATION:
				print "Error LDAP ldap.ldap.CONSTRAINT_VIOLATION for delete_s(dn) dn:[%s]" % (dn)
				pass
			except ldap.SERVER_DOWN:
				print "Error: asi spadl LDAP server"
				sys.exit(1)


def syncLDAProle (con, testOpt, verboseOpt, ldapcon, ldap_baseDn):
	# Vythahnem si seznam roli z databaze
	mesicNyni	= "%02d" % int(datetime.today().month)
	denNyni		= "%02d" % int(datetime.today().day)
	dnes = ( "%s-%s-%s" ) % (datetime.today().year, mesicNyni, denNyni)
	sql = "SELECT Uzivatel_id, Oblast_id, Text FROM SpravceOblasti JOIN TypSpravceOblasti ON TypSpravceOblasti.id = SpravceOblasti.TypSpravceOblasti_id WHERE od <= '%s' AND (do >= '%s' OR do is null)" % (dnes, dnes)
	rows, numRows = spustSql(con, sql, testOpt, verboseOpt)

	# do roles budem ukladat vysledky z databaze a to jak pro TECH, VV, tak pro SO-XXX, ZSO-XXX (XXX je cislo oblasti)
	roles = dict()
	if (numRows > 0):
		for row in rows:
			(userID, oblastID, role) = row
			userID = int(userID)
			role = str(role)

			# podle cisel oblasti, ale razeno dle LDAPu, tj napr SO-2 nebo ZSO-65
			if ((role == "SO") or (role == "ZSO")):
				ldapCN = "%s-%s" % (str(role), str(oblastID))
				if ldapCN in roles:
					roles[ldapCN].append(userID)
				else:
					roles[ldapCN] = [userID]

			# podle typu roli
			if role in roles:
				roles[role].append(userID)
			else:
				roles[role] = [userID]
	else:
		print "ERROR, z databaze nemame ani jeden radek s rolema"
		sys.exit(1)

	# do ldapRoles pridame uid co jsou v dane roli v ldapu prirazeni
	ldapRoles = dict()
	for role in roles.keys():
		ldapFilter = "(memberOf=cn=%s,ou=roles,dc=hkfree,dc=org)" % role
		ldap_attributes = ["member"]

		try:
			result_id = ldapcon.search(ldap_baseDn, ldap.SCOPE_SUBTREE, ldapFilter, ldap_attributes)
			while 1:
				try:
					result_type, result_data = ldapcon.result(result_id, 0)
				except ldap.NO_SUCH_OBJECT:
					raise DirectoryError("Distinguished name (%s) does not exist." % ldap_baseDn)
				if (result_data == []):
					break
				else:
					if result_type == ldap.RES_SEARCH_ENTRY:
						dn = result_data[0][0]
						m = re.search('uid=(.+?),ou=', dn)
						uid = int(m.group(1))	# vysledek z hledani v regexpu (.+?)
						if role ==  "ZSO" and uid == 393:
							print ldapFilter
							print dn
						
							print uid
						if role in ldapRoles:
							ldapRoles[role].append(uid)
						else:
							ldapRoles[role] = [uid]
		except ldap.LDAPError, e:
			print e

	# nyni mame v roles role z databaze, v ldapRoles role z LDAPu, projdeme je a najdem co pridat, smazat
	for role in roles.keys():
		dn = "cn=%s,ou=roles,dc=hkfree,dc=org" % (role)
		if role not in ldapRoles.keys():
			# role neni v LDAPu, je nutne ji zalozit
			print "Role [%s] neni v LDAPu. Zakladam ji vcetne clenu" % (role)
			newMembers = []			# zde si ulozime list member

			pocetMembers = len(roles[role])
			uid = roles[role].pop(0)

#			for uid in roles[role]:
			newMembers.append("uid=%s,ou=People,dc=hkfree,dc=org" % (uid))

			# atributy toho noveho dn
			attrs = {}
			attrs['cn'] = role
			attrs['description'] = role
			attrs['objectClass'] = 'groupOfNames'
			attrs['member'] = [member for member in newMembers]

			# pridat novou roli
			try:
				result = ldapcon.add(dn, ldap.modlist.addModlist(attrs))
			except ldap.TYPE_OR_VALUE_EXISTS:
				print "Error LDAP: TYPE_OR_VALUE_EXISTS [%s]" % (dn)
				pass
			except ldap.CONSTRAINT_VIOLATION:
				print "Error LDAP ldap.ldap.CONSTRAINT_VIOLATION"
				pass
			except ldap.SERVER_DOWN:
				print "Error: asi spadl LDAP server"
				sys.exit(1)

			# pridame klic do ldapRoles, protoze v ldapu neni, tak at nam to nehlasi error dale v kodu
			ldapRoles[role] = []

			if (len(roles[role]) > 0):
				# v LDAPu je asi bug, protoze pokud jsme rovnou sli pridavat cleny, tak LDAP sel regulerne na hubu. Se sleep v pohode.
				time.sleep(3)

		if (len(roles[role]) > 0):
			# nyni porovname seznamy UIDcek
			dbUIDs = roles[role]
			ldapUIDs = ldapRoles[role]

			# Pro ty co nejsou v LDAPU, tak ty tam pridame
			for dbUID in dbUIDs:
				if not dbUID in ldapUIDs:
					if not ((dbUID == 1) or (dbUID == 555)):	# UID 1 a 555 je specialni user, do LDAPu nemaji mit pristupy
						print "UID z db[%s] neni v LDAPu v roli [%s], pridavam ho" % (dbUID, role)
						member = "uid=%s,ou=People,dc=hkfree,dc=org" % (dbUID)
						modlist = [ (ldap.MOD_ADD, 'member', member) ]

						try:
							ldapcon.modify_s(dn, modlist)
						except ldap.TYPE_OR_VALUE_EXISTS:
							pass
						except ldap.CONSTRAINT_VIOLATION:
							pass
						except ldap.SERVER_DOWN:
							print "Error: asi spadl LDAP server"
							sys.exit(1)
						except ldap.NO_SUCH_OBJECT:
							print "Error: NO_SUCH_OBJECT ldap.modify_s dn:[%s] modlist:[%s]" % (dn, modlist)
		else:
			print "Uz dalsi clen neni k pridani"

		# Pro ty co jsou v LDAPU, ale ne v db, tak ty smazeme
		for ldapUID in ldapUIDs:
			if not ldapUID in dbUIDs:
				print "UID z LDAPu[%s] ve skupine [%s] neni v db, smazu ho ze skupiny." % (ldapUID, role)
				member = "uid=%s,ou=People,dc=hkfree,dc=org" % (ldapUID)
				modlist = [ (ldap.MOD_DELETE, 'member', member) ]

				try:
					ldapcon.modify_s(dn, modlist)
				except ldap.NO_SUCH_ATTRIBUTE:
					pass
				except ldap.NO_SUCH_OBJECT:
					print "Error: NO_SUCH_OBJECT ldap.modify_s dn:[%s] modlist:[%s]" % (dn, modlist)
				except ldap.SERVER_DOWN:
					print "Error: asi spadl LDAP server"
					sys.exit(1)

if __name__ == "__main__":
	main(sys.argv[1:])
