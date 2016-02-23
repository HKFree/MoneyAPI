#!/bin/bash
#
#
#


IPTABLESDIR="/opt/projects/money2/firewall-pravidla"
IPTABLEFILE="/opt/projects/money2/firewall-pravidla/iptables-final/rules-save-igw12"

NOW=`date +'%Y-%m-%d %H:%M:%S'`

echo "#" > $IPTABLEFILE
echo "# Vygenerovano dne [$NOW]" >> $IPTABLEFILE
echo "#" >> $IPTABLEFILE
echo "#" >> $IPTABLEFILE

#Nejdrive spojime NAT veci
cat $IPTABLESDIR/NAT >> $IPTABLEFILE
cat $IPTABLESDIR/AREAS_NAT >> $IPTABLEFILE

# Docasne DISABLOVANO 20160118
#cat $IPTABLESDIR/REDSYS >> $IPTABLEFILE

#Fix 31.7.2010 Kendy, chain REDSYS jiz negeneruje COMMIT
echo "COMMIT" >> $IPTABLEFILE


#Ted veci co nesouvisi s natem
echo "*filter" >> $IPTABLEFILE
echo ":INPUT ACCEPT [0:0]" >> $IPTABLEFILE
echo ":FORWARD ACCEPT [0:0]" >> $IPTABLEFILE
echo ":OUTPUT ACCEPT [0:0]" >> $IPTABLEFILE
echo ":SSH - [0:0]" >> $IPTABLEFILE

# Docasne DISABLOVANO 20160118
#cat $IPTABLESDIR/AREAS >> $IPTABLEFILE
cat $IPTABLESDIR/SMTP >> $IPTABLEFILE
cat $IPTABLESDIR/static/CUSTOM_FORWARD >> $IPTABLEFILE
cat $IPTABLESDIR/static/SYSTEM_FORWARD >> $IPTABLEFILE
cat $IPTABLESDIR/static/DNS >> $IPTABLEFILE
cat $IPTABLESDIR/static/FORWARD >> $IPTABLEFILE
cat $IPTABLESDIR/static/INPUT >> $IPTABLEFILE
cat $IPTABLESDIR/static/OUTPUT >> $IPTABLEFILE
cat $IPTABLESDIR/static/SSH >> $IPTABLEFILE
echo "COMMIT" >> $IPTABLEFILE

chmod o+r $IPTABLEFILE

