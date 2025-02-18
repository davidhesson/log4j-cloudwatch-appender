package com.arcadia.logging.log4j;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.arcadia.logging.log4j.CloudWatchDebugger.debug;

class CloudWatchLogService {
  private final String logGroupName;
  private final int retentionPeriodDays;
  private final String logStreamNamePrefix;
  private final AWSLogs awsLogs;
  private final LogStreamProvider logStreamProvider;
  private final AtomicReference<String> lastSequenceToken = new AtomicReference<>();

  CloudWatchLogService(String logGroupName, int retentionPeriodDays, String logStreamNamePrefix) {
    this(logGroupName, retentionPeriodDays, logStreamNamePrefix, AWSLogsClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain()).build(), Clock.systemUTC());
  }

  // visible for testing
  CloudWatchLogService(String logGroupName, int retentionPeriodDays, String logStreamNamePrefix, AWSLogs awsLogs, Clock clock) {
    this.logGroupName = logGroupName;
    this.retentionPeriodDays = retentionPeriodDays;
    this.logStreamNamePrefix = logStreamNamePrefix;
    this.awsLogs = awsLogs;
    new LogGroupProvider(awsLogs).ensureExists(logGroupName, retentionPeriodDays);
    logStreamProvider = new LogStreamProvider(awsLogs, clock);
  }

  @SuppressWarnings("OverlyBroadCatchBlock")
  void sendMessages(List<InputLogEvent> inputLogEvents) {
    try {
      PutLogEventsRequest request = new PutLogEventsRequest(logGroupName,
          logStreamProvider.getName(logStreamNamePrefix, logGroupName), inputLogEvents)
          .withSequenceToken(lastSequenceToken.get());
      send(request);
    } catch(Exception e) {
      CloudWatchDebugger.debug("Error while sending logs:", e);
    }
  }

  private void send(PutLogEventsRequest request) {
    try {
      PutLogEventsResult result = awsLogs.putLogEvents(request);
      lastSequenceToken.set(result.getNextSequenceToken());
    } catch(InvalidSequenceTokenException e) {
      CloudWatchDebugger.debug("InvalidSequenceTokenException while sending logs", e);
      request.setSequenceToken(e.getExpectedSequenceToken());
      PutLogEventsResult result = awsLogs.putLogEvents(request);
      lastSequenceToken.set(result.getNextSequenceToken());
    }
  }

}
