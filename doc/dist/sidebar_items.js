sidebarNodes={"modules":[{"id":"KafkaEx","functions":[{"id":"consumer_group_metadata/2","anchor":"consumer_group_metadata/2"},{"id":"create_worker/2","anchor":"create_worker/2"},{"id":"earliest_offset/3","anchor":"earliest_offset/3"},{"id":"fetch/3","anchor":"fetch/3"},{"id":"latest_offset/3","anchor":"latest_offset/3"},{"id":"metadata/1","anchor":"metadata/1"},{"id":"offset/4","anchor":"offset/4"},{"id":"offset_commit/2","anchor":"offset_commit/2"},{"id":"offset_fetch/2","anchor":"offset_fetch/2"},{"id":"produce/2","anchor":"produce/2"},{"id":"produce/4","anchor":"produce/4"},{"id":"start/2","anchor":"start/2"},{"id":"stop_streaming/1","anchor":"stop_streaming/1"},{"id":"stream/3","anchor":"stream/3"},{"id":"valid_consumer_group?/1","anchor":"valid_consumer_group?/1"}],"types":[{"id":"uri/0","anchor":"t:uri/0"},{"id":"worker_init/0","anchor":"t:worker_init/0"},{"id":"worker_setting/0","anchor":"t:worker_setting/0"}]},{"id":"KafkaEx.Compression","functions":[{"id":"compress/2","anchor":"compress/2"},{"id":"decompress/2","anchor":"decompress/2"},{"id":"snappy_decompress_chunk/2","anchor":"snappy_decompress_chunk/2"}],"types":[{"id":"attribute_t/0","anchor":"t:attribute_t/0"},{"id":"compression_type_t/0","anchor":"t:compression_type_t/0"}]},{"id":"KafkaEx.NetworkClient","functions":[{"id":"close_socket/1","anchor":"close_socket/1"},{"id":"create_socket/2","anchor":"create_socket/2"},{"id":"format_host/1","anchor":"format_host/1"},{"id":"send_async_request/2","anchor":"send_async_request/2"},{"id":"send_sync_request/3","anchor":"send_sync_request/3"}]},{"id":"KafkaEx.Protocol","functions":[{"id":"create_request/3","anchor":"create_request/3"},{"id":"error/1","anchor":"error/1"}]},{"id":"KafkaEx.Protocol.ConsumerMetadata","functions":[{"id":"create_request/3","anchor":"create_request/3"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.ConsumerMetadata.Response","functions":[{"id":"broker_for_consumer_group/2","anchor":"broker_for_consumer_group/2"}],"types":[{"id":"t/0","anchor":"t:t/0"}]},{"id":"KafkaEx.Protocol.Fetch","functions":[{"id":"create_request/8","anchor":"create_request/8"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.Fetch.Message"},{"id":"KafkaEx.Protocol.Fetch.Response"},{"id":"KafkaEx.Protocol.Metadata","functions":[{"id":"create_request/3","anchor":"create_request/3"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.Metadata.Broker","functions":[{"id":"connected?/1","anchor":"connected?/1"}],"types":[{"id":"t/0","anchor":"t:t/0"}]},{"id":"KafkaEx.Protocol.Metadata.PartitionMetadata"},{"id":"KafkaEx.Protocol.Metadata.Request"},{"id":"KafkaEx.Protocol.Metadata.Response","functions":[{"id":"broker_for_topic/4","anchor":"broker_for_topic/4"}]},{"id":"KafkaEx.Protocol.Metadata.TopicMetadata"},{"id":"KafkaEx.Protocol.Offset","functions":[{"id":"create_request/5","anchor":"create_request/5"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.Offset.Request"},{"id":"KafkaEx.Protocol.Offset.Response"},{"id":"KafkaEx.Protocol.OffsetCommit","functions":[{"id":"create_request/3","anchor":"create_request/3"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.OffsetCommit.Request"},{"id":"KafkaEx.Protocol.OffsetCommit.Response"},{"id":"KafkaEx.Protocol.OffsetFetch","functions":[{"id":"create_request/3","anchor":"create_request/3"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.OffsetFetch.Request"},{"id":"KafkaEx.Protocol.OffsetFetch.Response","functions":[{"id":"last_offset/1","anchor":"last_offset/1"}],"types":[{"id":"t/0","anchor":"t:t/0"}]},{"id":"KafkaEx.Protocol.Produce","functions":[{"id":"create_request/3","anchor":"create_request/3"},{"id":"parse_response/1","anchor":"parse_response/1"}]},{"id":"KafkaEx.Protocol.Produce.Message"},{"id":"KafkaEx.Protocol.Produce.Request"},{"id":"KafkaEx.Protocol.Produce.Response"},{"id":"KafkaEx.Server","functions":[{"id":"start_link/2","anchor":"start_link/2"}]},{"id":"KafkaEx.Server.State"},{"id":"KafkaEx.Supervisor","functions":[{"id":"init/1","anchor":"init/1"},{"id":"start_link/0","anchor":"start_link/0"}]},{"id":"KafkaExHandler"}],"exceptions":[{"id":"KafkaEx.ConnectionError","functions":[{"id":"exception/1","anchor":"exception/1"},{"id":"message/1","anchor":"message/1"}]}]}