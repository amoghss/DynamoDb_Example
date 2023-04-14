package com.implementation.dynamodb.repository.simple_primary_key;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.*;
import com.implementation.dynamodb.DynamodbApplicationTests;
import com.implementation.dynamodb.Helper;
import com.implementation.dynamodb.entity.simple_primary_key.UserEntity;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UserRepositoryTest extends DynamodbApplicationTests {

    @Autowired
    private DynamoDBMapper dynamoDBMapper;
    @Autowired
    private AmazonDynamoDB amazonDynamoDB;
    @Autowired
    private UserRepository userRepository;

//    @AfterAll
//    public static void dropDynamoDbTable(){
//        Helper.amazonDynamoDB().deleteTable("simpleUserTable");
//    }

    @BeforeAll
    public static void createDynamodbTable(){
        try {
            Helper.amazonDynamoDB().deleteTable("simpleUserTable");
        }catch (Exception e){
            // docker restart was done no table found
        }
        var attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("email").withAttributeType("S"));

        var keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("email").withKeyType(KeyType.HASH));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName("simpleUserTable")
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L));

        Helper.amazonDynamoDB().createTable(request);
    }
    @Test
    void saveAndBatchLoadTest(){
        var entityList = getMockEntityList1();
        userRepository.save(entityList.get(0));
        userRepository.save(entityList.get(1));
        userRepository.save(entityList.get(2));
        var result = userRepository.batchLoad(entityList.stream().map(UserEntity::getEmail).toList());
        Assert.assertEquals(entityList, result);
    }

    @Test
    void batchSaveAndBatchLoadTest(){
        var entityList = getMockEntityList2();
        userRepository.save(entityList.get(0));
        userRepository.save(entityList.get(1));
        userRepository.save(entityList.get(2));
        var result = userRepository.batchLoad(entityList.stream().map(UserEntity::getEmail).toList());
        Assert.assertEquals(entityList, result);
    }

    @Test
    void deleteTest(){
        var entityList = getMockEntityList3();
        userRepository.save(entityList.get(0));
        userRepository.delete(entityList.get(0).getEmail());

        var response =userRepository.load(entityList.get(0).getEmail());
        Assert.assertNull(response);
    }

    @Test
    void batchDeleteTest(){
        var entity = getMockEntityList4();
        userRepository.batchSave(entity);

        userRepository.batchDelete(entity.stream().map(UserEntity::getEmail).collect(Collectors.toList()));
        var response =userRepository.batchLoad(entity.stream().map(UserEntity::getEmail).toList());
        Assert.assertEquals(Collections.emptyList(), response);
    }

    private List<UserEntity> getMockEntityList4(){
        var entity = UserEntity.builder()
                .email("deleteUser@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .lastName("One")
                .age("23")
                .build();
        var entity2 = UserEntity.builder()
                .email("deleteUser2@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .age("23")
                .build();
        var entity3 = UserEntity.builder()
                .email("deleteUser3@test.com")
                .firstName("Tester")
                .lastName("Three")
                .build();

        var entityList = new ArrayList<UserEntity>();
        entityList.add(entity);
        entityList.add(entity2);
        entityList.add(entity3);
        return entityList;
    }

    private List<UserEntity> getMockEntityList3(){
        var entity = UserEntity.builder()
                .email("deleteTest@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .build();

        var entityList = new ArrayList<UserEntity>();
        entityList.add(entity);
        return entityList;
    }

    private List<UserEntity> getMockEntityList2(){
        var entity = UserEntity.builder()
                .email("Admin4@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .lastName("One")
                .age("23")
                .build();
        var entity2 = UserEntity.builder()
                .email("Admin5@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .age("23")
                .build();
        var entity3 = UserEntity.builder()
                .email("Admin6@test.com")
                .firstName("Tester")
                .lastName("Three")
                .build();

        var entityList = new ArrayList<UserEntity>();
        entityList.add(entity);
        entityList.add(entity2);
        entityList.add(entity3);
        return entityList;
    }

    private List<UserEntity> getMockEntityList1(){
        var entity = UserEntity.builder()
                .email("Admin@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .lastName("One")
                .age("23")
                .build();
        var entity2 = UserEntity.builder()
                .email("Admin2@test.com")
                .phone("1234567890")
                .firstName("Tester")
                .age("23")
                .build();
        var entity3 = UserEntity.builder()
                .email("Admin3@test.com")
                .firstName("Tester")
                .lastName("Three")
                .build();

        var entityList = new ArrayList<UserEntity>();
        entityList.add(entity);
        entityList.add(entity2);
        entityList.add(entity3);
        return entityList;
    }
}
