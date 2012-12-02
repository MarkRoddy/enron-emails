/**
 * enron-emails
 *
 * Required parameters:
 *
 * -param INPUT_PATH Input path for script data (e.g. s3n://hawk-example-data/tutorial/excite.log.bz2)
 * -param OUTPUT_PATH Output path for script data (e.g. s3n://my-output-bucket/enron-emails)
 */

/**
 * User-Defined Functions (UDFs)
 */
REGISTER '../udfs/python/enron-emails.py' USING streaming_python AS enronemails;

-- JSON loading approach taken from here:
-- http://eric.lubow.org/2011/hadoop/pig-queries-parsing-json-on-amazons-elastic-map-reduce-using-s3-data/
REGISTER $JARDIR/google-collect-1.0.jar;
REGISTER $JARDIR/json-simple-1.1.jar;
REGISTER $JARDIR/elephant-bird-1.2.1-SNAPSHOT.jar;
DEFINE JsonLoader com.twitter.elephantbird.PIG.LOAD.JsonLoader();
emails = load '$INPUT' using JsonLoader()
  AS (body, from, tos, ccs, bccs, date, message_id, subject);

emails = filter emails by message_id is not null;

/* Limit to 1,000 documents for local mode, or go bake a cake in the meanwhile */
emails = limit emails 100;
id_body = foreach emails generate message_id, body;

-- define test_stream `token_extractor.py` SHIP ('token_extractor.py');
-- cleaned_words = stream id_body through test_stream as (message_id:chararray, token_strings:chararray);
--token_records = foreach cleaned_words generate message_id, FLATTEN(TOKENIZE(token_strings)) as tokens;
cleaned_words = foreach id_body generate message_id FLATTEN(TOKENIZE(enronemails.tokenize(body))) as tokens;
    
/* Calculate the term count per document */
doc_word_totals = foreach (group token_records by (message_id, tokens)) generate
    flatten(group) as (message_id, token),
    COUNT_STAR(token_records) as doc_total;

/* Calculate the document size */
pre_term_counts = foreach (group doc_word_totals by message_id) generate
    group AS message_id,
    FLATTEN(doc_word_totals.(token, doc_total)) as (token, doc_total),
    SUM(doc_word_totals.doc_total) as doc_size;

/* Calculate the TF */
term_freqs = foreach pre_term_counts generate message_id as message_id,
    token as token,
    ((double)doc_total / (double)doc_size) AS term_freq;

/* Get count of documents using each token, for idf */
token_usages = foreach (group term_freqs by token) generate
    FLATTEN(term_freqs) as (message_id, token, term_freq),
    COUNT_STAR(term_freqs) as num_docs_with_token;

/* Get document count */
just_ids = foreach emails generate message_id;
ndocs = foreach (group just_ids all) generate COUNT_STAR(just_ids) as total_docs;

/* Note the use of Pig Scalars to calculate idf */
tfidf_all = foreach token_usages {
    idf  = LOG((double)ndocs.total_docs/(double)num_docs_with_token);
    tf_idf = (double)term_freq * idf;
    generate message_id as message_id,
        token as score,
        (chararray)tf_idf as value:chararray;
};

/* Get the top 10 Tf*Idf scores per message */
per_message_cassandra = foreach (group tfidf_all by message_id) {
    sorted = order tfidf_all by value desc;
    top_10_topics = limit sorted 10;
    generate group, top_10_topics.(score, value);
}

STORE per_message_cassandra into '$OUTFILE' using PigStorage();
-- store per_message_cassandra into 'cassandra://enron/email_topics' USING CassandraStorage();

/* This will give you some message_id keys to fetch in Cassandra, and some message bodies to compare topics to. */
samples = limit just_ids 10;
dump samples;
          
