package com.implementation.dynamodb.entity.single_table_design;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamoDBTable(tableName = "genericTable")
public class GenericEntity {
    @DynamoDBHashKey
    private String hashKey;
    @DynamoDBRangeKey
    @DynamoDBIndexRangeKey(globalSecondaryIndexName = "stateIndex")
    private String rangeKey;
    // Order Details.
    @DynamoDBAttribute
    private List<String> productIds;
    @DynamoDBAttribute
    private String subtotal;
    @DynamoDBIndexHashKey(globalSecondaryIndexName = "stateIndex")
    @DynamoDBAttribute
    private String shippingAddress;
    @DynamoDBAttribute
    private String nameOnOrder;
    @DynamoDBAttribute
    private String contactDetails;
    // User Details.
    @DynamoDBAttribute
    private String phone;
    @DynamoDBAttribute
    private String firstName;
    @DynamoDBAttribute
    private String lastName;
    @DynamoDBAttribute
    private String age;
}
