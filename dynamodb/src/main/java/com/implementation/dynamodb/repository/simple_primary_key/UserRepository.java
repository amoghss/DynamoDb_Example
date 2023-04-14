package com.implementation.dynamodb.repository.simple_primary_key;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.xspec.L;
import com.amazonaws.services.dynamodbv2.xspec.S;
import com.implementation.dynamodb.entity.complex_primary_key.OrderEntity;
import com.implementation.dynamodb.entity.simple_primary_key.UserEntity;
import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class UserRepository {
    @Autowired
    private DynamoDBMapper dynamoDBMapper;

    public void save(UserEntity entity){
        dynamoDBMapper.save(entity);
    }

    public void batchSave(List<UserEntity> userEntities){
        dynamoDBMapper.batchSave(userEntities);
    }

    public UserEntity load(String hashKey){
        var queryParameter = UserEntity.builder().email(hashKey).build();
        return dynamoDBMapper.load(queryParameter);
    }

    public List<UserEntity> batchLoad(List<String> hashKeys){
        var entityList = hashKeys.stream().map(p-> UserEntity.builder().email(p).build()).toList();
        var dynamoResponse = dynamoDBMapper.batchLoad(entityList);
        return (List<UserEntity>) (Object) dynamoResponse.get("simpleUserTable");
    }

    public void delete(String hashKey){
        var queryParameter = UserEntity.builder().email(hashKey).build();
        dynamoDBMapper.delete(queryParameter);
    }

    public void batchDelete(List<String> hashKey){
        var queryParameter = hashKey.stream().map(p-> UserEntity.builder().email(p).build()).toList();
        dynamoDBMapper.batchDelete(queryParameter);
    }
}
