-module(aws_sqs).

-export([send_message/1, receive_message/1, delete_message_batch/1, delete_message/1, extract_sqs_messages_from_xml/1]).

-include("aws_sqs.hrl").
-include_lib("fast_xml/include/fxml.hrl").

%%====================================================================
%% API
%%====================================================================
send_message(#sqs_send_message{host = Host, region = Region, queue_url = QueueUrl, path = Path, body = RawBody} = _) ->
    Body = http_uri:encode(RawBody),
    RequestParameters = "Action=SendMessage&MessageBody=" ++ Body ++ "&Version=2012-11-05",
    Response = httpc:request(get,
                  {QueueUrl ++ "?" ++ RequestParameters, sign_request("GET", "sqs", Host, Region, Path, RequestParameters)},
                  [{timeout, 60000}, {connect_timeout, 30000}],
                  []),
    {ok, {{Version, Code, ReasonPhrase}, Headers, ResponseBody}} = Response,
    XmlBody = fxml_stream:parse_element(erlang:list_to_binary(ResponseBody)),
    {ok, {{Version, Code, ReasonPhrase}, Headers, XmlBody}}.

receive_message(#sqs_receive_message{host = Host, region = Region, queue_url = QueueUrl, path = Path, attribute_names = AttributeNames, max_number_of_messages = MaxNumberOfMessages, message_attribute_name = MessageAttributeName, visibility_timeout = VisibilityTimeout, wait_time_second = WaitTimeSecond} = _) ->
    RequestParameters = "Action=ReceiveMessage&AttributeName=" ++ http_uri:encode(flatten(AttributeNames)) ++ "&MaxNumberOfMessages=" ++ integer_to_list(MaxNumberOfMessages) ++ "&MessageAttributeName=" ++ http_uri:encode(flatten(MessageAttributeName))  ++ "&Version=2012-11-05" ++ "&VisibilityTimeout=" ++ integer_to_list(VisibilityTimeout) ++ "&WaitTimeSeconds=" ++ integer_to_list(WaitTimeSecond),
    Response = httpc:request(get,
                  {QueueUrl ++ "?" ++ RequestParameters, sign_request("GET", "sqs", Host, Region, Path, RequestParameters)},
                  [{timeout, 60000}, {connect_timeout, 30000}],
                  []),
    {ok, {{Version, Code, ReasonPhrase}, Headers, ResponseBody}} = Response,
    XmlBody = fxml_stream:parse_element(erlang:list_to_binary(ResponseBody)),
    {ok, {{Version, Code, ReasonPhrase}, Headers, XmlBody}}.

delete_message(#sqs_delete_message{host = Host, region = Region, queue_url = QueueUrl, path = Path, receipt_handle = ReceiptHandle} = _) ->
    Body = http_uri:encode(ReceiptHandle),
    RequestParameters = "Action=DeleteMessage&ReceiptHandle=" ++ Body ++ "&Version=2012-11-05",
    Response = httpc:request(get,
                  {QueueUrl ++ "?" ++ RequestParameters, sign_request("GET", "sqs", Host, Region, Path, RequestParameters)},
                  [{timeout, 60000}, {connect_timeout, 30000}],
                  []),
    {ok, {{Version, Code, ReasonPhrase}, Headers, ResponseBody}} = Response,
    XmlBody = fxml_stream:parse_element(erlang:list_to_binary(ResponseBody)),
    {ok, {{Version, Code, ReasonPhrase}, Headers, XmlBody}}.

delete_message_batch(#sqs_delete_message_batch{ message_batch = []} = _) ->
    {ok, nothing_to_delete};
delete_message_batch(#sqs_delete_message_batch{host = Host, region = Region, queue_url = QueueUrl, path = Path, message_batch = MessageBatch} = _) ->
    BatchBody = message_batch_to_url(MessageBatch),
    RequestParameters = "Action=DeleteMessageBatch&" ++ BatchBody ++ "&Version=2012-11-05",
    Response = httpc:request(get,
                  {QueueUrl ++ "?" ++ RequestParameters, sign_request("GET", "sqs", Host, Region, Path, RequestParameters)},
                  [{timeout, 60000}, {connect_timeout, 30000}],
                  []),
    {ok, {{Version, Code, ReasonPhrase}, Headers, ResponseBody}} = Response,
    XmlBody = fxml_stream:parse_element(erlang:list_to_binary(ResponseBody)),
    {ok, {{Version, Code, ReasonPhrase}, Headers, XmlBody}}.

extract_sqs_messages_from_xml(#xmlel{name = <<"ReceiveMessageResponse">>, children = [
            #xmlel{ name = <<"ReceiveMessageResult">>, children = Messages},
            #xmlel{ name = <<"ResponseMetadata">>, children = _MetaData}]} = _Xml) ->
    do_extract_sqs_messages_from_xml(Messages).

do_extract_sqs_messages_from_xml([]) ->
    [];
do_extract_sqs_messages_from_xml([H|T]) ->
    [message_xml_to_sqs_message(H)| do_extract_sqs_messages_from_xml(T)].

message_xml_to_sqs_message(#xmlel{name = <<"Message">>, children = Children} = _Xml) ->
    do_message_xml_to_sqs_message(#sqs_message{}, Children).

do_message_xml_to_sqs_message(Message, []) ->
    Message;
do_message_xml_to_sqs_message(Message, [H|T]) ->
    case H of
        #xmlel{ name = <<"MessageId">>, children = [{xmlcdata, MesasgeId}]} ->
            Message1 = #sqs_message{
                message_id = MesasgeId,
                receipt_handle = Message#sqs_message.receipt_handle,
                md5_of_body = Message#sqs_message.md5_of_body,
                body = Message#sqs_message.body
                },
            do_message_xml_to_sqs_message(Message1, T);
        #xmlel{ name = <<"ReceiptHandle">>, children = [{xmlcdata, ReceiptHandle}]} ->
            Message1 = #sqs_message{
                message_id = Message#sqs_message.message_id,
                receipt_handle = ReceiptHandle,
                md5_of_body = Message#sqs_message.md5_of_body,
                body = Message#sqs_message.body
                },
            do_message_xml_to_sqs_message(Message1, T);
        #xmlel{ name = <<"MD5OfBody">>, children = [{xmlcdata, MD5OfBody}]} ->
            Message1 = #sqs_message{
                message_id = Message#sqs_message.message_id,
                receipt_handle = Message#sqs_message.receipt_handle,
                md5_of_body = MD5OfBody,
                body = Message#sqs_message.body
                },
            do_message_xml_to_sqs_message(Message1, T);
        #xmlel{ name = <<"Body">>, children = [{xmlcdata, Body}]} ->
            Message1 = #sqs_message{
                message_id = Message#sqs_message.message_id,
                receipt_handle = Message#sqs_message.receipt_handle,
                md5_of_body = Message#sqs_message.md5_of_body,
                body = Body
                },
            do_message_xml_to_sqs_message(Message1, T);
        _ ->
            Message
    end.

message_batch_to_url([]) ->
    "";
message_batch_to_url([H|T]) ->
    Raw = "",
    Counter = 1,
    do_message_batch_to_url(Raw, Counter, H, T).

do_message_batch_to_url(Raw, Counter, #sqs_message{message_id = ID, receipt_handle = ReceiptHandle} = _, []) ->
    Raw ++ "DeleteMessageBatchRequestEntry." ++ integer_to_list(Counter) ++ ".Id=" ++ http_uri:encode(binary_to_list(ID)) ++ "&DeleteMessageBatchRequestEntry." ++ integer_to_list(Counter) ++ ".ReceiptHandle=" ++ http_uri:encode(binary_to_list(ReceiptHandle));
do_message_batch_to_url(Raw, Counter, #sqs_message{message_id = ID, receipt_handle = ReceiptHandle} = _, [HT | T]) ->
    Raw1 = Raw ++ "DeleteMessageBatchRequestEntry." ++ integer_to_list(Counter) ++ ".Id=" ++ http_uri:encode(binary_to_list(ID)) ++ "&DeleteMessageBatchRequestEntry." ++ integer_to_list(Counter) ++ ".ReceiptHandle=" ++ http_uri:encode(binary_to_list(ReceiptHandle)) ++ "&",
    Counter1 = Counter + 1,
    do_message_batch_to_url(Raw1, Counter1, HT, T).

flatten(List) when is_list(List) ->
    lists:foldl(
        fun(X, Acc) when Acc =/= "" ->
          Acc ++ "," ++ X;
        (X, Acc) ->
            Acc ++ X
        end, "", List);
flatten(_) ->
    "".


utcString() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = erlang:universaltime(),
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0wT~2..0w~2..0w~2..0wZ",[Year, Month, Day, Hour, Minute, Second])).

dateString() ->
    {{Year, Month, Day}, _} = erlang:universaltime(),
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0w",[Year, Month, Day])).

sign(Key, Msg) ->
    crypto:hmac(sha256, Key, Msg).

getSignatureKey(Key, DateStamp, RegionName, ServiceName) ->
    KDate = sign("AWS4" ++ Key, DateStamp),
    KRegion = sign(KDate, RegionName),
    KService = sign(KRegion, ServiceName),
    sign(KService, "aws4_request").

sign_request(Method, Service, Host, Region, Path, RequestParameters) ->
    AccessKey = os:getenv("AWS_SQS_ACCESS_KEY_ID"),
    SecretKey = os:getenv("AWS_SQS_SECRET_ACCESS_KEY"),

    AmzDate = utcString(),
    DateStamp = dateString(),

    CanonicalURI = Path,

    CanonicalQueryString = RequestParameters,
    CanonicalHeaders = "host:" ++ Host ++ "\n" ++ "x-amz-date:" ++ AmzDate ++ "\n",
    SignedHeaders = "host;x-amz-date",
    <<X:256/big-unsigned-integer>> = crypto:hash(sha256,""),
    Sha256Hex = lists:flatten(io_lib:format("~64.16.0b", [X])),
    PayloadHash = Sha256Hex,
    CanonicalRequest = Method ++ "\n" ++ CanonicalURI ++ "\n" ++ CanonicalQueryString ++ "\n" ++ CanonicalHeaders ++ "\n" ++ SignedHeaders ++ "\n" ++ PayloadHash,
    Algorithm = "AWS4-HMAC-SHA256",
    CredentialScope = DateStamp ++ "/" ++ Region ++ "/" ++ Service ++ "/" ++ "aws4_request",

    <<X1:256/big-unsigned-integer>> = crypto:hash(sha256, CanonicalRequest),
    StringToSign = Algorithm ++ "\n" ++ AmzDate ++ "\n" ++ CredentialScope ++ "\n" ++ lists:flatten(io_lib:format("~64.16.0b", [X1])),

    SigningKey = getSignatureKey(SecretKey, DateStamp, Region, Service),
    <<X2:256/big-unsigned-integer>> = crypto:hmac(sha256, SigningKey, StringToSign),
    Signature = lists:flatten(io_lib:format("~64.16.0b", [X2])),
    AuthorizationHeader = Algorithm ++ " " ++ "Credential=" ++ AccessKey ++ "/" ++ CredentialScope ++ ", " ++ "SignedHeaders=" ++ SignedHeaders ++ ", " ++ "Signature=" ++ Signature,
    [{"x-amz-date", AmzDate}, {"Authorization", AuthorizationHeader}].
