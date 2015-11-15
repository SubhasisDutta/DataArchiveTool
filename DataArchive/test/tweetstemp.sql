CREATE TABLE tweetstemp (
	id uuid,
	posteddate timestamp,
	pickupdate timestamp,
	acronyms text,
	boundingboxcoordinates text,
	boundingboxtype text,
	coordinates text,
	countrycode text,
	currentuserretweetid bigint,
	description text,
	displayurl text,
	emoticons text,
	entrytime bigint,
	expandedurl text,
	favoritecount int,
	favouritescount int,
	followerscount int,
	friendscount int,
	hashtags LIST<text>,
	inreplytoscreenname text,
	inreplytostatusid bigint,
	inreplytouserid bigint,
	isfavorited boolean,
	isgeoenabled boolean,
	ispossiblysensitive boolean,
	isprotected boolean,
	isretweeeted boolean,
	isretweet boolean,
	isretweetedbyme boolean,
	isverified boolean,
	language text,
	listedcount int,
	location text,
	name text,
	placefullname text,
	placeid text,
	placename text,
	profileimageurl text,
	profileimageurlhttps text,
	quotedstatusid bigint,
	retweetcount int,
	screenname text,
	sentiments int,
	source text,
	statusescount int,
	streetaddress text,
	text text,
	timezone text,
	topics LIST<text>,
	tweetid bigint,
	uniqueid text,
	url text,
	userid bigint,
	PRIMARY KEY (id)
) WITH bloom_filter_fp_chance = 0.01
AND comment = ''
AND dclocal_read_repair_chance = 0.1
AND default_time_to_live = 0
AND gc_grace_seconds = 864000
AND index_interval = 128
AND memtable_flush_period_in_ms = 0
AND populate_io_cache_on_flush = false
AND read_repair_chance = 0.0
AND replicate_on_write = true
AND speculative_retry = '99.0PERCENTILE'
AND caching = 'KEYS_ONLY'
AND compression = {
	'sstable_compression' : 'LZ4Compressor'
}
AND compaction = {
	'class' : 'SizeTieredCompactionStrategy'
};