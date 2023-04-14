package com.implementation.dynamodb.entity.complex_primary_key;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.*;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamoDBTable(tableName = "complexOrderTable")
@EqualsAndHashCode
public class OrderEntity {
    @DynamoDBHashKey
    private String email;
    @DynamoDBRangeKey
    private String orderId;
    @DynamoDBAttribute
    private List<String> productIds;
    @DynamoDBAttribute
    private String subtotal;
    @DynamoDBAttribute
    private String shippingAddress;
    @DynamoDBAttribute
    private String nameOnOrder;
    @DynamoDBAttribute
    private String contactDetails;
}
