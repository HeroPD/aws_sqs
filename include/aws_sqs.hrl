-record(sqs_send_message , {
    host  = "" :: list(),
    region = "" :: list(),
    queue_url = "" :: list(),
    path = "" :: list(),
    body = "" :: list()
}).

-record(sqs_receive_message , {
    host  = "" :: list(),
    region = "" :: list(),
    queue_url = "" :: list(),
    path = "" :: list(),
    attribute_names = ["All"] :: list(),
    max_number_of_messages = 1 :: integer(),
    message_attribute_name = ["All"] :: list(),
    visibility_timeout = 2 :: integer(),
    wait_time_second = 1 :: integer()
}).

-record(sqs_delete_message , {
    host  = "" :: list(),
    region = "" :: list(),
    queue_url = "" :: list(),
    path = "" :: list(),
    receipt_handle = "" :: list()
}).

-record(sqs_delete_message_batch , {
    host  = "" :: list(),
    region = "" :: list(),
    queue_url = "" :: list(),
    path = "" :: list(),
    message_batch = [] :: [sqs_message()]
}).

-record(sqs_message , {
    message_id = "" :: list(),
    receipt_handle = "" :: list(),
    md5_of_body = "" :: list(),
    body = "" :: list()
}).

-type(sqs_message() :: #sqs_message{}).

