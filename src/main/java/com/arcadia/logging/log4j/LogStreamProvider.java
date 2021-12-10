package com.arcadia.logging.log4j;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogStream;

import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static com.arcadia.logging.log4j.CloudWatchDebugger.debug;

class LogStreamProvider {
  private static final String LOG_STREAM_UUID = UUID.randomUUID().toString();

  // This used to be the stream name (now uses UUID)
  //private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM");

  private final AWSLogs awsLogs;
  private final Clock clock;
  private String lastName;

  LogStreamProvider(AWSLogs awsLogs, Clock clock) {
    this.awsLogs = awsLogs;
    this.clock = clock;
  }

  String getName(String prefix, String logGroupName) {
    String name = buildLogStreamName(prefix);
    if(!Objects.equals(lastName, name)) {
      ensureExists(logGroupName, name);
    }
    return name;
  }

  private void ensureExists(String logGroupName, String name) {
    DescribeLogStreamsRequest request = new DescribeLogStreamsRequest()
        .withLogGroupName(logGroupName)
        .withLogStreamNamePrefix(name);

    DescribeLogStreamsResult streamsResult = awsLogs.describeLogStreams(request);
    if(streamsResult != null) {
      Optional<LogStream> existing = streamsResult.getLogStreams().stream()
          .filter(logStream -> logStream.getLogStreamName().equalsIgnoreCase(name))
          .findFirst();

      if(!existing.isPresent()) {
        CloudWatchDebugger.debug("Creates LogStream: " + name + " in LogGroup: " + logGroupName);
        awsLogs.createLogStream(new CreateLogStreamRequest().withLogGroupName(logGroupName)
            .withLogStreamName(name));
      }
    }
    lastName = name;
  }

  private String buildLogStreamName(String prefix) {
    //String date = FORMATTER.format(LocalDate.now(clock));
    return prefix == null || prefix.trim().isEmpty()
        ? LOG_STREAM_UUID
        : prefix + "/" + LOG_STREAM_UUID;
  }
}
