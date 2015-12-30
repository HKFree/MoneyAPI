#! /usr/bin/python
# -*- coding: utf-8 -*-
#
# author: Kendy 12/2015

import MySQLdb as mdb

def spustSql (con, sql, testOpt, verboseOpt):
	cur = con.cursor()

	try:
		if verboseOpt == True: print "Poustim sql dotaz [%s] nad databazi" % ( sql ); print
		if testOpt == False:
			cur.execute(sql)
			con.commit()
			rows = cur.fetchall()
			numRows = cur.rowcount
	except mdb.Error, e:
		try:
			print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
		except IndexError:
			print "MySQL Error: %s" % str(e)

	return (rows, numRows)
