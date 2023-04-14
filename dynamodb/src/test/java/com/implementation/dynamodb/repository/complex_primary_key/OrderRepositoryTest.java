package com.implementation.dynamodb.repository.complex_primary_key;

import com.amazonaws.services.dynamodbv2.model.*;
import com.implementation.dynamodb.DynamodbApplicationTests;
import com.implementation.dynamodb.Helper;
import com.implementation.dynamodb.entity.complex_primary_key.OrderEntity;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.internal.matchers.Or;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OrderRepositoryTest extends DynamodbApplicationTests {

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void loadTest(){
        var expectedResponse = craeteMockForTest1();
        var actualResponse = orderRepository.load(expectedResponse.getEmail(), expectedResponse.getOrderId());
        Assert.assertEquals(expectedResponse, actualResponse);
    }

    @Test
    void batchLoadTest(){
        var expectedResponse = craeteMockForTest2();
        orderRepository.batchSave(expectedResponse);
        var queryParams = expectedResponse.stream().map(p-> Arrays.asList(new String[]{p.getEmail(), p.getOrderId()})).toList();
        var actualResponse = orderRepository.batchLoad(queryParams);
        Assert.assertEquals(expectedResponse.size(), actualResponse.size());
    }

    @Test
    void queryTest(){
        var mock = craeteMockForTest3();
        orderRepository.batchSave(mock);
        var actualResponseAdmin = orderRepository.queryforUser("QueryAdmin@test.com");
        var actualResponseUser = orderRepository.queryforUser("QueryUser@test.com");
        Assert.assertEquals(3, actualResponseAdmin.size());
        Assert.assertEquals(1, actualResponseUser.size());
    }

    @Test
    void queryWithFilterTest(){
        var mock = craeteMockForTest4();
        orderRepository.batchSave(mock);
        var actualResponseAdmin = orderRepository.getOrderMadeByPerson("QueryFilterAdmin@test.com", "Admin");
        var actualResponseAdmin2 = orderRepository.getOrderMadeByPerson("QueryFilterAdmin@test.com", "Admin2");
        var actualResponseSuperAdmin = orderRepository.getOrderMadeByPerson("QueryFilterAdmin@test.com", "SuperAdmin");
        var actualResponseUser = orderRepository.getOrderMadeByPerson("QueryFilterUser@test.com", "Test");
        Assert.assertEquals(1, actualResponseAdmin.size());
        Assert.assertEquals(1, actualResponseAdmin2.size());
        Assert.assertEquals(1, actualResponseSuperAdmin.size());
        Assert.assertEquals(0, actualResponseUser.size());
    }

    private List<OrderEntity> craeteMockForTest4(){
        List<OrderEntity> entityList = new ArrayList<>();
        entityList.add(OrderEntity.builder()
                .email("QueryFilterAdmin@test.com")
                .orderId("ORDER#100002")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin2")
                .contactDetails("1234567890")
                .build());
        entityList.add(OrderEntity.builder()
                .email("QueryFilterAdmin@test.com")
                .orderId("ORDER#100003")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("SuperAdmin")
                .contactDetails("1234567890")
                .build());
        entityList.add(OrderEntity.builder()
                .email("QueryFilterAdmin@test.com")
                .orderId("ORDER#100004")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin")
                .contactDetails("1234567890")
                .build());
        entityList.add(OrderEntity.builder()
                .email("QueryFilterUser@test.com")
                .orderId("ORDER#200001")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("10000")
                .shippingAddress("Noida")
                .nameOnOrder("Test User")
                .contactDetails("1234567890")
                .build());
        return entityList;
    }

    private List<OrderEntity> craeteMockForTest3(){
        List<OrderEntity> entityList = new ArrayList<>();
        entityList.add(OrderEntity.builder()
                .email("QueryAdmin@test.com")
                .orderId("ORDER#100002")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin2")
                .contactDetails("1234567890")
                .build());
        entityList.add(OrderEntity.builder()
                .email("QueryAdmin@test.com")
                .orderId("ORDER#100003")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("SuperAdmin")
                .contactDetails("1234567890")
                .build());
        entityList.add(OrderEntity.builder()
                .email("QueryAdmin@test.com")
                .orderId("ORDER#100004")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin")
                .contactDetails("1234567890")
                .build());
        entityList.add(OrderEntity.builder()
                .email("QueryUser@test.com")
                .orderId("ORDER#200001")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("10000")
                .shippingAddress("Noida")
                .nameOnOrder("Test User")
                .contactDetails("1234567890")
                .build());

        return entityList;
    }

    private List<OrderEntity> craeteMockForTest2(){
        List<OrderEntity> entityList = new ArrayList<>();
        var one = (OrderEntity.builder()
                .email("Admin@test.com")
                .orderId("ORDER#100002")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin2")
                .contactDetails("1234567890")
                .build());
        var two = (OrderEntity.builder()
                .email("Admin@test.com")
                .orderId("ORDER#100003")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("SuperAdmin")
                .contactDetails("1234567890")
                .build());
        var three =(OrderEntity.builder()
                .email("Admin@test.com")
                .orderId("ORDER#100004")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin")
                .contactDetails("1234567890")
                .build());
        var four =(OrderEntity.builder()
                .email("User@test.com")
                .orderId("ORDER#200001")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("10000")
                .shippingAddress("Noida")
                .nameOnOrder("Test User")
                .contactDetails("1234567890")
                .build());
        entityList.add(one);
        entityList.add(two);
        entityList.add(three);
        entityList.add(four);
        return entityList;
    }

    private OrderEntity craeteMockForTest1(){
        var entity1 =OrderEntity.builder()
                .email("Admin@test.com")
                .orderId("ORDER#100001")
                .productIds(Arrays.asList(new String[]{"123", "124"}))
                .subtotal("1000")
                .shippingAddress("Noida")
                .nameOnOrder("Admin")
                .contactDetails("1234567890")
                .build();

        orderRepository.save(entity1);

        return entity1;
    }





    //    @AfterAll
//    public static void dropDynamoDbTable(){
//        Helper.amazonDynamoDB().deleteTable("simpleUserTable");
//    }

    @BeforeAll
    public static void createDynamodbTable(){
        try {
            Helper.amazonDynamoDB().deleteTable("complexOrderTable");
        }catch (Exception e){
            // docker restart was done no table found
        }
        var attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("email").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("orderId").withAttributeType("S"));

        var keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName("email").withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("orderId").withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName("complexOrderTable")
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L));

        Helper.amazonDynamoDB().createTable(request);
    }
}
