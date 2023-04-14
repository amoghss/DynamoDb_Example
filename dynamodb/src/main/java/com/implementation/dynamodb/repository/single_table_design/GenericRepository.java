package com.implementation.dynamodb.repository.single_table_design;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.implementation.dynamodb.entity.complex_primary_key.OrderEntity;
import com.implementation.dynamodb.entity.single_table_design.GenericEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class GenericRepository {
    @Autowired
    private DynamoDBMapper dynamoDBMapper;

    public void save(GenericEntity entity){
        dynamoDBMapper.save(entity);
    }

    public void batchSave(List<GenericEntity> userEntities){
        dynamoDBMapper.batchSave(userEntities);
    }

    public GenericEntity load(String hashKey, String orderId){
        var queryParameter = GenericEntity.builder().hashKey(hashKey).rangeKey(orderId).build();
        return dynamoDBMapper.load(queryParameter);
    }

    public List<GenericEntity> batchLoad(List<List<String>> keySet){
        var entityList = keySet.stream().map(p-> GenericEntity.builder().hashKey(p.get(0)).rangeKey(p.get(1)).build()).toList();
        var dynamoResponse = dynamoDBMapper.batchLoad(entityList);
        return (List<GenericEntity>) (Object) dynamoResponse.get("complexOrderTable");
    }

    public void delete(String hashKey, String sortKey){
        var queryParameter = GenericEntity.builder().hashKey(hashKey).rangeKey(sortKey).build();
        dynamoDBMapper.delete(queryParameter);
    }

    public void batchDelete(List<List<String>> keySet){
        var queryParameter = keySet.stream().map(p-> GenericEntity.builder().hashKey(p.get(0)).rangeKey(p.get(1)).build()).toList();
        dynamoDBMapper.batchDelete(queryParameter);
    }

    public List<GenericEntity> getOrderList(String hashKey){
        DynamoDBQueryExpression dynamoDBQueryExpression = new DynamoDBQueryExpression();
        dynamoDBQueryExpression.withHashKeyValues(GenericEntity.builder().hashKey(hashKey).build());
        List<GenericEntity> response = dynamoDBMapper.query(GenericEntity.class, dynamoDBQueryExpression);
        return response.stream().filter(p->p.getRangeKey().startsWith("ORDER")).toList();
    }

    public List<GenericEntity> getOrderMadeByPerson(String hashKey, String name){
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":v1", new AttributeValue().withS(name));
        var dynamoDBQueryExpression = new DynamoDBQueryExpression()
            .withHashKeyValues(GenericEntity.builder().hashKey(hashKey).build())
            .withFilterExpression("nameOnOrder =:v1")
            .withExpressionAttributeValues(eav);
        PaginatedList<GenericEntity> response = dynamoDBMapper
                .query(GenericEntity.class, dynamoDBQueryExpression);
        return new ArrayList<>(response);
    }

    public List<GenericEntity> scanByLocation(String address) {
        var eav = new HashMap<String, AttributeValue>();
        eav.put(":val1", new AttributeValue().withS(address));
        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("shippingAddress =:val1")
                .withExpressionAttributeValues(eav);
        var result = dynamoDBMapper.scan(GenericEntity.class, scanExpression);
        return new ArrayList<>(result);
    }

    public List<GenericEntity> getByLocationGsi(String hashKey){
        var dynamoDBQueryExpression = new DynamoDBQueryExpression()
                .withHashKeyValues(GenericEntity.builder().shippingAddress(hashKey).build())
                .withIndexName("stateIndex")
                .withConsistentRead(false);
        PaginatedList<GenericEntity> response = dynamoDBMapper
                .query(GenericEntity.class, dynamoDBQueryExpression);
        return new ArrayList<>(response);
    }

}
