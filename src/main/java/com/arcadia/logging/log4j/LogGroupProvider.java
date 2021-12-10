package com.arcadia.logging.log4j;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import java.util.Optional;

import static com.arcadia.logging.log4j.CloudWatchDebugger.debug;

class LogGroupProvider {

  private final AWSLogs awsLogs;

  LogGroupProvider(AWSLogs awsLogs) {
    this.awsLogs = awsLogs;
  }

  void ensureExists(String name, int retentionPeriodDays) {
    DescribeLogGroupsRequest request = new DescribeLogGroupsRequest().withLogGroupNamePrefix(name);

    DescribeLogGroupsResult groupsResult = awsLogs.describeLogGroups(request);
    if(groupsResult != null) {
      Optional<LogGroup> existing = groupsResult.getLogGroups().stream()
          .filter(logGroup -> logGroup.getLogGroupName().equalsIgnoreCase(name))
          .findFirst();

      if(!existing.isPresent()) {
        CloudWatchDebugger.debug("Creates LogGroup: " + name);
        awsLogs.createLogGroup(new CreateLogGroupRequest().withLogGroupName(name));

        if (retentionPeriodDays != CloudWatchAppender.DEFAULT_INFINITE_RETENTION) {
          CloudWatchDebugger.debug("Setting log group period to " + retentionPeriodDays + " days for group " + name);
          awsLogs.putRetentionPolicy(new PutRetentionPolicyRequest(name, retentionPeriodDays));
        }
      }
    }
  }
}
