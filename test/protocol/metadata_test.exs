defmodule KafkaEx.Protocol.Metadata.Test do
  use ExUnit.Case, async: true

  test "create_request with no topics creates a valid metadata request" do
    good_request = <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 0::32>>
    request = KafkaEx.Protocol.Metadata.create_request(1, "foo", [])
    assert request == good_request
  end

  test "create_request with a single topic creates a valid metadata request" do
    good_request =
      <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 1::32, 3::16, "bar"::binary>>

    request = KafkaEx.Protocol.Metadata.create_request(1, "foo", ["bar"])
    assert request == good_request
  end

  test "create_request with a multiple topics creates a valid metadata request" do
    good_request =
      <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 3::32, 3::16, "bar"::binary,
        3::16, "baz"::binary, 4::16, "food"::binary>>

    request =
      KafkaEx.Protocol.Metadata.create_request(1, "foo", ["bar", "baz", "food"])

    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response =
      <<0::32, 1::32, 0::32, 3::16, "foo"::binary, 9092::32, 1::32, 0::16,
        3::16, "bar"::binary, 1::32, 0::16, 0::32, 0::32, 0::32, 1::32, 0::32>>

    expected_response = %KafkaEx.Protocol.Metadata.Response{
      brokers: [
        %KafkaEx.Protocol.Metadata.Broker{host: "foo", node_id: 0, port: 9092}
      ],
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %KafkaEx.Protocol.Metadata.PartitionMetadata{
              error_code: :no_error,
              isrs: [0],
              leader: 0,
              partition_id: 0,
              replicas: []
            }
          ],
          topic: "bar"
        }
      ]
    }

    assert expected_response ==
             KafkaEx.Protocol.Metadata.parse_response(response)
  end

  test "Response.broker_for_topic returns correct broker for a topic" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    socket = %KafkaEx.Socket{socket: fake_socket}

    metadata = %KafkaEx.Protocol.Metadata.Response{
      brokers: [
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9092,
          port: 9092
        },
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9093,
          port: 9093
        }
      ],
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %KafkaEx.Protocol.Metadata.PartitionMetadata{
              error_code: :no_error,
              isrs: [0],
              leader: 9092,
              partition_id: 0,
              replicas: []
            }
          ],
          topic: "bar"
        }
      ]
    }

    brokers = [
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9092,
        socket: socket
      },
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9091,
        socket: socket
      }
    ]

    assert KafkaEx.Protocol.Metadata.Response.broker_for_topic(
             metadata,
             brokers,
             "bar",
             0
           ) == %KafkaEx.Protocol.Metadata.Broker{
             host: "192.168.0.1",
             port: 9092,
             socket: socket
           }

    Port.close(fake_socket)
  end

  test "Response.broker_for_topic returns nil when the topic is not found" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    socket = %KafkaEx.Socket{socket: fake_socket}

    metadata = %KafkaEx.Protocol.Metadata.Response{
      brokers: [
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9092,
          port: 9092
        },
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9093,
          port: 9093
        }
      ],
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %KafkaEx.Protocol.Metadata.PartitionMetadata{
              error_code: :no_error,
              isrs: [0],
              leader: 9092,
              partition_id: 0,
              replicas: []
            }
          ],
          topic: "bar"
        }
      ]
    }

    brokers = [
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9092,
        socket: socket
      },
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9091,
        socket: socket
      }
    ]

    assert KafkaEx.Protocol.Metadata.Response.broker_for_topic(
             metadata,
             brokers,
             "foo",
             0
           ) == nil
  end

  test "Response.broker_for_topic returns nil when the partition is not found" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    socket = %KafkaEx.Socket{socket: fake_socket}

    metadata = %KafkaEx.Protocol.Metadata.Response{
      brokers: [
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9092,
          port: 9092
        },
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9093,
          port: 9093
        }
      ],
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %KafkaEx.Protocol.Metadata.PartitionMetadata{
              error_code: :no_error,
              isrs: [0],
              leader: 9092,
              partition_id: 0,
              replicas: []
            }
          ],
          topic: "bar"
        }
      ]
    }

    brokers = [
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9092,
        socket: socket
      },
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9091,
        socket: socket
      }
    ]

    assert KafkaEx.Protocol.Metadata.Response.broker_for_topic(
             metadata,
             brokers,
             "bar",
             1
           ) == nil

    Port.close(fake_socket)
  end

  test "Response.broker_for_topic returns nil when a brokers socket is closed" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    socket = %KafkaEx.Socket{socket: fake_socket}
    Port.close(fake_socket)

    metadata = %KafkaEx.Protocol.Metadata.Response{
      brokers: [
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9092,
          port: 9092
        },
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9093,
          port: 9093
        }
      ],
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %KafkaEx.Protocol.Metadata.PartitionMetadata{
              error_code: :no_error,
              isrs: [0],
              leader: 9092,
              partition_id: 0,
              replicas: []
            }
          ],
          topic: "bar"
        }
      ]
    }

    brokers = [
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9093,
        socket: socket
      },
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9091,
        socket: socket
      }
    ]

    assert KafkaEx.Protocol.Metadata.Response.broker_for_topic(
             metadata,
             brokers,
             "bar",
             0
           ) == nil
  end

  test "Response.broker_for_topic returns nil when a matching broker is not found" do
    fake_socket = Port.open({:spawn, "cat"}, [])
    socket = %KafkaEx.Socket{socket: fake_socket}

    metadata = %KafkaEx.Protocol.Metadata.Response{
      brokers: [
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9092,
          port: 9092
        },
        %KafkaEx.Protocol.Metadata.Broker{
          host: "192.168.0.1",
          node_id: 9093,
          port: 9093
        }
      ],
      topic_metadatas: [
        %KafkaEx.Protocol.Metadata.TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %KafkaEx.Protocol.Metadata.PartitionMetadata{
              error_code: :no_error,
              isrs: [0],
              leader: 9092,
              partition_id: 0,
              replicas: []
            }
          ],
          topic: "bar"
        }
      ]
    }

    brokers = [
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9093,
        socket: socket
      },
      %KafkaEx.Protocol.Metadata.Broker{
        host: "192.168.0.1",
        port: 9091,
        socket: socket
      }
    ]

    assert KafkaEx.Protocol.Metadata.Response.broker_for_topic(
             metadata,
             brokers,
             "bar",
             0
           ) == nil

    Port.close(fake_socket)
  end

  test "Response.partitions_for_topic returns a list of partition ids" do
    alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
    alias KafkaEx.Protocol.Metadata.TopicMetadata

    partition_metadata = %KafkaEx.Protocol.Metadata.PartitionMetadata{
      partition_id: 0
    }

    metadata = %MetadataResponse{
      topic_metadatas: [
        %TopicMetadata{
          error_code: :no_error,
          partition_metadatas: [
            %{partition_metadata | partition_id: 0},
            %{partition_metadata | partition_id: 1},
            %{partition_metadata | partition_id: 2},
            %{partition_metadata | partition_id: 3}
          ],
          topic: "bar"
        }
      ]
    }

    assert [0, 1, 2, 3] ==
             MetadataResponse.partitions_for_topic(metadata, "bar")
  end

  test "Response.partitions_for_topic returns an empty list for a topic " <>
         "that does not exist" do
    alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse

    metadata = %MetadataResponse{topic_metadatas: []}

    assert [] == MetadataResponse.partitions_for_topic(metadata, "foo")
  end
end
