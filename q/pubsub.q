/ pubsub.q

fhIBM:`:data/IBM.csv
fhGOOG:`:data/GOOG.csv
fhAAPL:`:data/AAPL.csv

loadTestData:{[fh;sym]
  show "Loading test data, file=", (string fh), ", length=", string hcount fh;
  `Sym`Date xkey update Sym:sym from ("SDFFFFIF"; enlist ",")0:fh
  }
  
dailybars:loadTestData[fhIBM;`IBM],loadTestData[fhAAPL;`AAPL],loadTestData[fhGOOG;`GOOG]
show dailybars
show select Rows:count i by Sym from dailybars
show "Loaded ", (string count dailybars), " rows"

/ table of open subscriptions
subs:([handle:()];time:`datetime$();id:`symbol$();table:`symbol$();subq:`symbol$();upf:())
/ `subs insert (0;.z.Z;`gfeng;`test;`testsubq;{x});
/ show subs

/ functions for pubsub
kdb_sub:{[id;table;subq;upf]
	h:.z.w;
	ha:.z.a;
	he:.z.u;
	
	show "XXXX from host:", (string ha), ", user: ", (string he);
	
	show "Subscribe: handle=", (string h), ", id=", (string id), ", table=", (string table), ", subq=", (string subq), ", upd=", (string upd);
	
	`subs insert (h;.z.Z;id;table;subq;value string upf);
	show subs;
	show "Running subscription query: ", string subq;
	d:value string subq;
	(`h`id`st`ut`d)!(h;id;.z.Z;`s;d)
	}

kdb_insert:{[table;rows]
	show "Inserting ", (string count first rows), " rows into ", (string table);
	table insert rows;
	data:(keys table) xkey flip (cols table)!rows;
	kdb_notify[table;`i;data;0];
	}
	
kdb_delete:{[table;rows]
	show "Deleting ", (string count first rows), " rows from ", (string table);
	
	}
	
kdb_update:{[table;rows]
	show "Updating ", (string count first rows), "rows in ", (string table);
	data:(keys table) xkey flip (cols table)!rows;
	table upsert data;
	kdb_notify[table;`u;data;0];
	}
	
kdb_notify:{[table;ut;payload;sync]
	targets: exec handle from subs where table=table;
	show "Notifying: table=", (string table), "|", (string ut), ", targets=", (string count targets);
	
	n:0;
	do[count targets;
		h:targets[n];
		s:subs[h];
		upf:s[`upf];
		d:upf[payload];
		data:(`h`id`st`ut`d)!(h;s[`id];.z.Z;ut;d);
		show " handle: ", (string h);
		$[sync;h data;(neg h) data];
		n:n+1;
		];
	}
	
kdb_close:{[h]
	show "Closing subscription: handle=", (string h);
	delete from `subs where handle=h;
	show subs;
	}
	
kdb_send:{[x]
	h:first exec handle from subs;
	(neg h) x;
	}


sattr:{[t]
 c:first cols t;
 a:`g`u 1=n:count keys t;
 t:n!@[;c;a#]0!t;
 t}
/ table to hold active and inactive connection information
handle:sattr 1!flip `h`active`user`host`address`time!"ibss*p"$\:()

/ hook close to clean up subs
/ .z.pc:{dna_close[x]};
/ record new client connection
.z.po:{[h]`handle upsert (h;1b;.z.u;.Q.host .z.a;"i"$0x0 vs .z.a;.z.P);}
.z.po 0i / simulate opening of 0

/ mark client connection as inactive
.z.pc:{[h]
	`handle upsert `h`active`time!(h;0b;.z.P);
	kdb_close[h];
	}

upd:{
	show "xxxx received upd: ", (string x);
	(`acked;.z.Z)
  }

/ test data
/ kdb_insert[`dailybars;(`IBM`IBM;(.z.D;.z.D-1);(1.0 1.2);(2.0 2.1);(3.0 3.1);(4.0 4.1);(5.0 5.1);(6.0 6.1)
/ kdb_insert[`dailybars;(`AAPL`AAPL;(.z.D;.z.D-1);(1.0 1.2);(2.0 2.1);(3.0 3.1);(4.0 4.1);(5.0 5.1);(6.0 6.1)
/ kdb_insert[`dailybars;(`GOOG`GOOG;(.z.D;.z.D-1);(1.0 1.2);(2.0 2.1);(3.0 3.1);(4.0 4.1);(5.0 5.1);(6.0 6.1)

