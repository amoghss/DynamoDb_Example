package com.implementation.dynamodb.repository.complex_primary_key;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.implementation.dynamodb.entity.complex_primary_key.OrderEntity;
import com.implementation.dynamodb.entity.single_table_design.GenericEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class OrderRepository {
    @Autowired
    private DynamoDBMapper dynamoDBMapper;

    public void save(OrderEntity entity){
        dynamoDBMapper.save(entity);
    }

    public void batchSave(List<OrderEntity> userEntities){
        dynamoDBMapper.batchSave(userEntities);
    }

    public OrderEntity load(String hashKey, String orderId){
        var queryParameter = OrderEntity.builder().email(hashKey).orderId(orderId).build();
        return dynamoDBMapper.load(queryParameter);
    }

    public List<OrderEntity> batchLoad(List<List<String>> keySet){
        var entityList = keySet.stream().map(p-> OrderEntity.builder().email(p.get(0)).orderId(p.get(1)).build()).toList();
        var dynamoResponse = dynamoDBMapper.batchLoad(entityList);
        return (List<OrderEntity>) (Object) dynamoResponse.get("complexOrderTable");
    }

    public void delete(String hashKey, String sortKey){
        var queryParameter = OrderEntity.builder().email(hashKey).orderId(sortKey).build();
        dynamoDBMapper.delete(queryParameter);
    }

    public void batchDelete(List<List<String>> keySet){
        var queryParameter = keySet.stream().map(p-> OrderEntity.builder().email(p.get(0)).orderId(p.get(1)).build()).toList();
        dynamoDBMapper.batchDelete(queryParameter);
    }

    public List<OrderEntity> queryforUser(String hashKey){
        DynamoDBQueryExpression dynamoDBQueryExpression = new DynamoDBQueryExpression();
        dynamoDBQueryExpression.withHashKeyValues(OrderEntity.builder().email(hashKey).build());
        var response = dynamoDBMapper.query(OrderEntity.class, dynamoDBQueryExpression);
        return new ArrayList<>(response);
    }

    public List<OrderEntity> getOrderMadeByPerson(String hashKey, String name){
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":v1", new AttributeValue().withS(name));
        var dynamoDBQueryExpression = new DynamoDBQueryExpression()
                .withHashKeyValues(OrderEntity.builder().email(hashKey).build())
                .withFilterExpression("nameOnOrder =:v1")
                .withExpressionAttributeValues(eav);
        var response = dynamoDBMapper.query(OrderEntity.class, dynamoDBQueryExpression);
        return new ArrayList<>(response);
    }
}
