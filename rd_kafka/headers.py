from cffi import FFI


ffi = FFI()
ffi.cdef(
    # Most of this copied verbatim from librdkafka/rdkafka.h:
    """
    typedef enum rd_kafka_type_t {
        RD_KAFKA_PRODUCER, RD_KAFKA_CONSUMER, ... } rd_kafka_type_t;
    typedef ... rd_kafka_t;
    typedef ... rd_kafka_topic_t;
    typedef ... rd_kafka_conf_t;
    typedef ... rd_kafka_topic_conf_t;
    typedef ... rd_kafka_queue_t;
    typedef enum {RD_KAFKA_RESP_ERR_NO_ERROR,
                  RD_KAFKA_RESP_ERR__PARTITION_EOF, ...} rd_kafka_resp_err_t;

    const char *rd_kafka_err2str (rd_kafka_resp_err_t err);
    rd_kafka_resp_err_t rd_kafka_errno2err (int errnox);

    typedef struct rd_kafka_message_s {
        rd_kafka_resp_err_t err;   /* Non-zero for error signaling. */
        rd_kafka_topic_t *rkt;     /* Topic */
        int32_t partition;         /* Partition */
        void   *payload;           /* err==0: Message payload
                                    * err!=0: Error string */
        size_t  len;               /* err==0: Message payload length
                                    * err!=0: Error string length */
        void   *key;               /* err==0: Optional message key */
        size_t  key_len;           /* err==0: Optional message key length */
        int64_t offset;            /* Message offset (or offset for error
                                    * if err!=0 if applicable). */
        void  *_private;           /* rdkafka private pointer: DO NOT MODIFY */
    } rd_kafka_message_t;
    void rd_kafka_message_destroy (rd_kafka_message_t *rkmessage);

    typedef enum {RD_KAFKA_CONF_OK, ...  } rd_kafka_conf_res_t;
    rd_kafka_conf_t *rd_kafka_conf_new (void);
    rd_kafka_conf_t *rd_kafka_conf_dup (const rd_kafka_conf_t *conf);
    void rd_kafka_conf_destroy (rd_kafka_conf_t *conf);
    rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf,
                                           const char *name,
                                           const char *value,
                                           char *errstr,
                                           size_t errstr_size);
    void rd_kafka_conf_set_dr_msg_cb (rd_kafka_conf_t *conf,
                                      void (*dr_msg_cb) (
                                          rd_kafka_t *rk,
                                          const rd_kafka_message_t * rkmessage,
                                          void *opaque));

    rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void);
    rd_kafka_topic_conf_t *rd_kafka_topic_conf_dup (const rd_kafka_topic_conf_t
                                                    *conf);
    void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf);
    rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf,
                                                 const char *name,
                                                 const char *value,
                                                 char *errstr,
                                                 size_t errstr_size);
    void rd_kafka_topic_conf_set_partitioner_cb (
                rd_kafka_topic_conf_t *topic_conf,
                int32_t (*partitioner) (
                    const rd_kafka_topic_t *rkt,
                    const void *keydata,
                    size_t keylen,
                    int32_t partition_cnt,
                    void *rkt_opaque,
                    void *msg_opaque));
    int rd_kafka_topic_partition_available (const rd_kafka_topic_t *rkt,
                                            int32_t partition);

    rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf,
                              char *errstr, size_t errstr_size);
    void rd_kafka_destroy (rd_kafka_t *rk);

    rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic,
                                          rd_kafka_topic_conf_t *conf);
    void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt);
    const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt);

    #define RD_KAFKA_PARTITION_UA ...

    rd_kafka_queue_t *rd_kafka_queue_new (rd_kafka_t *rk);
    void rd_kafka_queue_destroy (rd_kafka_queue_t *rkqu);

    #define RD_KAFKA_OFFSET_BEGINNING ...
    #define RD_KAFKA_OFFSET_END ...
    #define RD_KAFKA_OFFSET_TAIL_BASE ... /* internal: do not use */

    int rd_kafka_consume_start_queue (rd_kafka_topic_t *rkt, int32_t partition,
                                      int64_t offset, rd_kafka_queue_t *rkqu);
    int rd_kafka_consume_stop (rd_kafka_topic_t *rkt, int32_t partition);
    rd_kafka_message_t *rd_kafka_consume_queue (rd_kafka_queue_t *rkqu,
                                                int timeout_ms);
    ssize_t rd_kafka_consume_batch_queue (rd_kafka_queue_t *rkqu,
                                          int timeout_ms,
                                          rd_kafka_message_t **rkmessages,
                                          size_t rkmessages_size);
    int rd_kafka_consume_callback_queue (rd_kafka_queue_t *rkqu,
                                         int timeout_ms,
                                         void (*consume_cb) (
                                                rd_kafka_message_t *rkmessage,
                                                void *opaque),
                                         void *opaque);

    #define RD_KAFKA_MSG_F_COPY ...

    int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partitition,
                          int msgflags,
                          void *payload, size_t len,
                          const void *key, size_t keylen,
                          void *msg_opaque);

    typedef struct rd_kafka_metadata_broker {
            int32_t     id;             /* Broker Id */
            char       *host;           /* Broker hostname */
            int         port;           /* Broker listening port */
    } rd_kafka_metadata_broker_t;

    typedef struct rd_kafka_metadata_partition {
            int32_t     id;             /* Partition Id */
            rd_kafka_resp_err_t err;    /* Partition error reported by broker */
            int32_t     leader;         /* Leader broker */
            int         replica_cnt;    /* Number of brokers in 'replicas' */
            int32_t    *replicas;       /* Replica brokers */
            int         isr_cnt;        /* Number of ISR brokers in 'isrs' */
            int32_t    *isrs;           /* In-Sync-Replica brokers */
    } rd_kafka_metadata_partition_t;

    typedef struct rd_kafka_metadata_topic {
            char       *topic;          /* Topic name */
            int         partition_cnt;  /* Number of partitions in 'partitions' */
            struct rd_kafka_metadata_partition *partitions; /* Partitions */
            rd_kafka_resp_err_t err;    /* Topic error reported by broker */
    } rd_kafka_metadata_topic_t;

    typedef struct rd_kafka_metadata {
            int         broker_cnt;     /* Number of brokers in 'brokers' */
            struct rd_kafka_metadata_broker *brokers;  /* Brokers */
            int         topic_cnt;      /* Number of topics in 'topics' */
            struct rd_kafka_metadata_topic *topics;    /* Topics */
            int32_t     orig_broker_id; /* Broker originating this metadata */
            char       *orig_broker_name; /* Name of originating broker */
    } rd_kafka_metadata_t;

    rd_kafka_resp_err_t
    rd_kafka_metadata (rd_kafka_t *rk, int all_topics,
                       rd_kafka_topic_t *only_rkt,
                       const struct rd_kafka_metadata **metadatap,
                       int timeout_ms);
    void rd_kafka_metadata_destroy (const struct rd_kafka_metadata *metadata);

    int rd_kafka_poll (rd_kafka_t *rk, int timeout_ms);
    int rd_kafka_outq_len (rd_kafka_t *rk);
    """)
lib = ffi.verify("#include <librdkafka/rdkafka.h>", libraries=['rdkafka'])



