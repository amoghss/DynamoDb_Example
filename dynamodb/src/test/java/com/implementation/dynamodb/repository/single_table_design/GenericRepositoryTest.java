package com.implementation.dynamodb.repository.single_table_design;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.*;
import com.implementation.dynamodb.DynamodbApplicationTests;
import com.implementation.dynamodb.Helper;
import com.implementation.dynamodb.entity.single_table_design.GenericEntity;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class GenericRepositoryTest extends DynamodbApplicationTests {

    @Autowired
    private GenericRepository genericRepository;

    @Test
    void getOrderOfUserTest(){
        var actualResponseAppleUser = genericRepository.getOrderList("apple@fan.com");
        var actualResponseSamsungUser = genericRepository.getOrderList("samsung@fan.com");
        var actualResponseRedmiUser = genericRepository.getOrderList("redmi@fan.com");

        Assert.assertEquals(4, actualResponseAppleUser.size());
        Assert.assertEquals(3, actualResponseSamsungUser.size());
        Assert.assertEquals(1, actualResponseRedmiUser.size());
    }

    @Test
    void getOrderByNameTest(){
        var actualResponseAppleUser1 = genericRepository.getOrderMadeByPerson("apple@fan.com", "Member1");
        var actualResponseAppleUser2 = genericRepository.getOrderMadeByPerson("apple@fan.com", "Member2");
        var actualResponseSamsungUser1 = genericRepository.getOrderMadeByPerson("samsung@fan.com", "Member1");
        var actualResponseSamsungUser2 = genericRepository.getOrderMadeByPerson("samsung@fan.com", "Member2");
        var actualResponseRedmiUser1 = genericRepository.getOrderMadeByPerson("redmi@fan.com", "Member1");
        var actualResponseRedmiUser2 = genericRepository.getOrderMadeByPerson("redmi@fan.com", "Member2");

        Assert.assertEquals(4, actualResponseAppleUser1.size());
        Assert.assertEquals(0, actualResponseAppleUser2.size());
        Assert.assertEquals(1, actualResponseSamsungUser1.size());
        Assert.assertEquals(2, actualResponseSamsungUser2.size());
        Assert.assertEquals(1, actualResponseRedmiUser1.size());
        Assert.assertEquals(0, actualResponseRedmiUser2.size());
    }

    @Test
    void getAllRecordsCount() {
        var actualResult = genericRepository.scanByLocation("Noida");
        Assert.assertEquals(8, actualResult.size());
    }

    @Test
    void getAllRecordsCountWithGsi() {
        var actualResult = genericRepository.getByLocationGsi("Noida");
        Assert.assertEquals(8, actualResult.size());
    }

    private static List<GenericEntity> saveUser(){
        var entityList = new ArrayList<GenericEntity>();
        entityList.add(GenericEntity.builder()
                .hashKey("apple@fan.com")
                .rangeKey("USER#123121923")
                .phone("1234567890")
                .firstName("Apple")
                .lastName("User")
                .age("23")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("samsung@fan.com")
                .rangeKey("USER#123121233")
                .phone("1234567890")
                .firstName("Samsung")
                .lastName("User")
                .age("23")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("redmi@fan.com")
                .rangeKey("USER#123121244")
                .firstName("Redmi")
                .lastName("Fan")
                .age("23")
                .build());

        return entityList;
    }

    private static List<GenericEntity> createOrders(){
        List<GenericEntity> entityList = new ArrayList<>();
        entityList.add(GenericEntity.builder()
                .hashKey("apple@fan.com")
                .rangeKey("ORDER#100000")
                .productIds(Arrays.asList(new String[]{"Iphone", "Apple Watch"}))
                .subtotal("1,00,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member1")
                .contactDetails("1234567890")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("apple@fan.com")
                .rangeKey("ORDER#100001")
                .productIds(Arrays.asList(new String[]{"MackBook Pro", "iPen"}))
                .subtotal("2,75,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member1")
                .contactDetails("1234567890")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("apple@fan.com")
                .rangeKey("ORDER#100002")
                .productIds(Arrays.asList(new String[]{"Apple Care"}))
                .subtotal("1,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member1")
                .contactDetails("1234567890")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("apple@fan.com")
                .rangeKey("ORDER#100003")
                .productIds(Arrays.asList(new String[]{"Mac Mini", "Ipad Pro"}))
                .subtotal("1,70,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member1")
                .contactDetails("1234567890")
                .build());

        entityList.add(GenericEntity.builder()
                .hashKey("samsung@fan.com")
                .rangeKey("ORDER#200000")
                .productIds(Arrays.asList(new String[]{"S23 Ultra", "Fast Charger", "Wireless Charger", "Galaxy Buds"}))
                .subtotal("1,60,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member1")
                .contactDetails("1234567890")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("samsung@fan.com")
                .rangeKey("ORDER#200001")
                .productIds(Arrays.asList(new String[]{"Apple Watch"}))
                .subtotal("2,75,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member2")
                .contactDetails("1234567890")
                .build());
        entityList.add(GenericEntity.builder()
                .hashKey("samsung@fan.com")
                .rangeKey("ORDER#200002")
                .productIds(Arrays.asList(new String[]{"Mi Charger"}))
                .subtotal("1,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member2")
                .contactDetails("1234567890")
                .build());

        entityList.add(GenericEntity.builder()
                .hashKey("redmi@fan.com")
                .rangeKey("ORDER#200000")
                .productIds(Arrays.asList(new String[]{"Mi Tablet"}))
                .subtotal("15,000")
                .shippingAddress("Noida")
                .nameOnOrder("Member1")
                .contactDetails("1234567890")
                .build());

        return entityList;
    }

    @BeforeAll
    public static void createDynamodbTable(){
        try {
            Helper.amazonDynamoDB().deleteTable("genericTable");
        }catch (Exception e){
            // docker restart was done no table found
        }
        var attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("hashKey").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("rangeKey").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("shippingAddress").withAttributeType("S"));

        var keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("hashKey").withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("rangeKey").withKeyType(KeyType.RANGE));

        var gsiKeySchema = new ArrayList<KeySchemaElement>();
        gsiKeySchema.add(new KeySchemaElement().withAttributeName("shippingAddress").withKeyType(KeyType.HASH));
        gsiKeySchema.add(new KeySchemaElement().withAttributeName("rangeKey").withKeyType(KeyType.RANGE));

        var gsi = new GlobalSecondaryIndex()
                .withIndexName("stateIndex")
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L))
                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                .withKeySchema(gsiKeySchema);

        CreateTableRequest request = new CreateTableRequest()
                .withTableName("genericTable")
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withGlobalSecondaryIndexes(gsi)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L));

        Helper.amazonDynamoDB().createTable(request);
        Helper.dynamoDBMapper().batchSave(createOrders());
        Helper.dynamoDBMapper().batchSave(saveUser());
    }
}
