package hu.blackbelt.judo.runtime.core.dao.rdbms;

import com.google.common.collect.ImmutableList;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.meta.psm.namespace.Model;
import hu.blackbelt.judo.meta.psm.runtime.PsmModel;
import hu.blackbelt.judo.meta.psm.support.PsmModelResourceSupport;
import hu.blackbelt.judo.runtime.core.dao.rdbms.custom.Gps;
import hu.blackbelt.judo.runtime.core.dao.rdbms.custom.StringToGpsConverter;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoExtension;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDaoFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceFixture;
import hu.blackbelt.judo.runtime.core.dao.rdbms.fixture.RdbmsDatasourceSingetonExtension;
import hu.blackbelt.model.northwind.Demo;
import hu.blackbelt.structured.map.proxy.MapHolder;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import sdk.demo.services.*;
import sdk.demo.types.Priority;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static hu.blackbelt.judo.dao.api.Payload.asPayload;
import static hu.blackbelt.judo.sdk.query.Filter.THIS_NAME;
import static hu.blackbelt.judo.sdk.query.StringFilter.matches;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(RdbmsDatasourceSingetonExtension.class)
@ExtendWith(RdbmsDaoExtension.class)
@Slf4j
public class StatementExecutorTest {

    public static final String DEMO_GPS_TYPE = "demo.types.GPS";

    public static final String DEMO_SERVICE_CATEGORY_INFO = "demo.services.CategoryInfo";
    public static final String DEMO_SERVICE_PRODUCT_INFO = "demo.services.ProductInfo";
    public static final String DEMO_SERVICE_INTERNATIONAL_ORDER_INFO = "demo.services.InternationalOrderInfo";
    public static final String DEMO_INTERNAL_AP = "demo.InternalUser";
    public static final String DEMO_SERVICE_SHIPPER_INFO = "demo.services.ShipperInfo";

    public static final String DEMO_SERVICE_ORDER_INFO = "demo.services.OrderInfo";
    public static final String DEMO_SERVICE_ORDER_ITEM = "demo.services.OrderItem";

    protected String getModelName() {
        return "northwind";
    }

    protected Model getPsmModel() {
        PsmModel psmModel = new Demo().fullDemo();
        return PsmModelResourceSupport.psmModelResourceSupportBuilder().resourceSet(psmModel.getResourceSet()).uri(psmModel.getUri()).build().getStreamOf(Model.class).findFirst().get();
    }

    @BeforeEach
    public void setup(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.setIgnoreSdk(false);
        daoFixture.init(getPsmModel(), datasourceFixture);
        assertTrue(daoFixture.isInitialized(), "DAO initialized");
        final EDataType gpsType = daoFixture.getAsmUtils().resolve(DEMO_GPS_TYPE).map(dataType -> (EDataType) dataType).get();
        RdbmsDaoFixture.DATA_TYPE_MANAGER.registerCustomType(gpsType, Gps.class.getName(), Collections.singleton(new StringToGpsConverter()));
    }

    @AfterEach
    public void teardown(RdbmsDaoFixture daoFixture, RdbmsDatasourceFixture datasourceFixture) {
        daoFixture.dropDatabase();
    }

    private UUID init(RdbmsDaoFixture daoFixture) {
        final EClass categoryInfoClass = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_CATEGORY_INFO).get();

        final Payload category1 = daoFixture.getDao().create(categoryInfoClass, asPayload(((MapHolder) buildCategory1()).toMap()), null);
        final Payload category2 = daoFixture.getDao().create(categoryInfoClass, asPayload(((MapHolder) buildCategory2()).toMap()), null);
        final Payload category3 = daoFixture.getDao().create(categoryInfoClass, asPayload(((MapHolder) buildCategory3()).toMap()), null);

        final EClass shipperInfoClass = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_SHIPPER_INFO).get();
        final Payload shipper = daoFixture.getDao().create(shipperInfoClass, asPayload(((MapHolder) buildShipper()).toMap()), null);

        final EClass productInfoClass = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_PRODUCT_INFO).get();
        final Payload product1 = daoFixture.getDao().create(productInfoClass, asPayload(((MapHolder) buildProduct1(CategoryInfo.builder().with__identifier(category1.getAs(UUID.class, daoFixture.getUuid().getName())).build())).toMap()), null);
        final Payload product2 = daoFixture.getDao().create(productInfoClass, asPayload(((MapHolder) buildProduct2(CategoryInfo.builder().with__identifier(category3.getAs(UUID.class, daoFixture.getUuid().getName())).build())).toMap()), null);
        final Payload product3 = daoFixture.getDao().create(productInfoClass, asPayload(((MapHolder) buildProduct3(CategoryInfo.builder().with__identifier(category1.getAs(UUID.class, daoFixture.getUuid().getName())).build())).toMap()), null);

        final EClass orderInfoClass = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_INTERNATIONAL_ORDER_INFO).get();
        final Payload order = daoFixture.getDao().create(orderInfoClass, asPayload(((MapHolder) buildInternationalOrder(ShipperInfo.builder().with__identifier(shipper.getAs(UUID.class, daoFixture.getUuid().getName())).build(),
                ProductInfo.builder().with__identifier(product1.getAs(UUID.class, daoFixture.getUuid().getName())).build(),
                ProductInfo.builder().with__identifier(product2.getAs(UUID.class, daoFixture.getUuid().getName())).build(),
                ProductInfo.builder().with__identifier(product3.getAs(UUID.class, daoFixture.getUuid().getName())).build())).toMap()), null);

        return order.getAs(UUID.class, daoFixture.getUuid().getName());
    }

    private void cleanup(RdbmsDaoFixture daoFixture, final Collection<UUID> ids) {
        final EClass orderInfo = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_ORDER_INFO).get();
        ids.forEach(id -> daoFixture.getDao().delete(orderInfo, id));
    }

    @Test
    public void testSingleOrderInsertQueryDelete(RdbmsDaoFixture daoFixture) {
        final UUID id = init(daoFixture);

        final EClass _internationalOrderInfoQuery = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_INTERNATIONAL_ORDER_INFO).get();

        final EClass _internalAP = daoFixture.getAsmUtils().getClassByFQName(DEMO_INTERNAL_AP).get();
        final Optional<EReference> _lastTwoWeekOrders = _internalAP.getEAllReferences().stream().filter(r -> "LastTwoWeekOrders".equals(r.getName())).findAny();

        final long startTs = System.currentTimeMillis();
        final Collection<? extends Map<String, Object>> results;
        if (_lastTwoWeekOrders.isPresent()) {
            results = daoFixture.getDao().getAllReferencedInstancesOf(_lastTwoWeekOrders.get(), _lastTwoWeekOrders.get().getEReferenceType());
        } else {
            results = daoFixture.getDao().getAllReferencedInstancesOf(_lastTwoWeekOrders.get(), _internationalOrderInfoQuery);
        }

        final long endTs = System.currentTimeMillis();
        log.debug("Results returned in {} ms:\n{}", (endTs - startTs), results);

        assertEquals(results.size(), 1);
        final Map<String, Object> orderInfo = results.iterator().next();

        assertEquals(id, orderInfo.get(daoFixture.getUuid().getName()));
        assertEquals(buildShipper().getCompanyName(), orderInfo.get("shipperName"));

        // TODO - workaround because zoned datetime is not supported yet (JNG-1586)
        final OffsetDateTime expectedOrderDate = buildInternationalOrder(null, null, null, null).getOrderDate().withOffsetSameInstant(ZoneOffset.UTC);
        assertEquals(expectedOrderDate, RdbmsDaoFixture.DATA_TYPE_MANAGER.getCoercer().coerce(orderInfo.get("orderDate"), OffsetDateTime.class).withOffsetSameInstant(ZoneOffset.UTC));
        assertEquals(buildInternationalOrder(null, null, null, null).getPriority().getOrdinal(), orderInfo.get("priority"));
        assertEquals(buildShipper().getLocation(), orderInfo.get("shipperLocation"));

        final Collection<Map<String, Object>> categories = (Collection<Map<String, Object>>) orderInfo.get("categories");
        final Collection<Map<String, Object>> items = (Collection<Map<String, Object>>) orderInfo.get("items");
        final Collection<Map<String, Object>> discountedItemsOutOfStock = (Collection<Map<String, Object>>) orderInfo.get("discountedItemsOutOfStock");
        assertEquals(2, categories.size());
        assertEquals(4, items.size());
        assertEquals(1, discountedItemsOutOfStock.size());
        final Optional<Map<String, Object>> category1Ofcategories = categories.stream().filter(c -> Objects.equals(c.get("categoryName"), buildCategory1().getCategoryName())).findAny();
        final Optional<Map<String, Object>> category3Ofcategories = categories.stream().filter(c -> Objects.equals(c.get("categoryName"), buildCategory3().getCategoryName())).findAny();
        assertTrue(category1Ofcategories.isPresent());
        assertTrue(category3Ofcategories.isPresent());
        final Optional<Map<String, Object>> item1 = items.stream().filter(c -> Objects.equals(c.get("productName"), buildProduct1(null).getProductName())).findAny();
        final Optional<Map<String, Object>> item2 = items.stream().filter(c -> Objects.equals(c.get("productName"), buildProduct2(null).getProductName())).findAny();
        final Optional<Map<String, Object>> item3 = items.stream().filter(c -> Objects.equals(c.get("productName"), buildProduct3(null).getProductName()) && Objects.equals(c.get("quantity"), 6)).findAny();
        final Optional<Map<String, Object>> item4 = items.stream().filter(c -> Objects.equals(c.get("productName"), buildProduct3(null).getProductName()) && Objects.equals(c.get("quantity"), 2)).findAny();
        final Optional<Map<String, Object>> discountedItemOutOfStock1 = discountedItemsOutOfStock.stream().filter(c -> Objects.equals(c.get("productName"), buildProduct2(null).getProductName())).findAny();
        assertTrue(item1.isPresent());
        assertTrue(item2.isPresent());
        assertTrue(item3.isPresent());
        assertTrue(item4.isPresent());
        assertTrue(discountedItemOutOfStock1.isPresent());

        final OrderItem expectedItem1 = ((List<OrderItem>) buildInternationalOrder(null, null, null, null).getItems()).get(0);
        final OrderItem expectedItem2 = ((List<OrderItem>) buildInternationalOrder(null, null, null, null).getItems()).get(1);
        final OrderItem expectedItem3 = ((List<OrderItem>) buildInternationalOrder(null, null, null, null).getItems()).get(2);
        final OrderItem expectedItem4 = ((List<OrderItem>) buildInternationalOrder(null, null, null, null).getItems()).get(3);

        assertEquals(expectedItem1.getUnitPrice(), item1.get().get("unitPrice"));
        assertEquals(expectedItem1.getQuantity(), item1.get().get("quantity"));
        assertEquals(expectedItem1.getUnitPrice() * expectedItem1.getQuantity() * (1 - expectedItem1.getDiscount()), item1.get().get("price"));
        assertEquals(expectedItem1.getDiscount(), item1.get().get("discount"));
        assertEquals(buildProduct1(null).getProductName(), item1.get().get("productName"));
        assertEquals(category1Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) item1.get().get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildCategory1().getCategoryName(), ((Map<String, Object>) item1.get().get("category")).get("categoryName"));
        assertEquals(category1Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) ((Map<String, Object>) item1.get().get("product")).get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildProduct1(null).getProductName(), ((Map<String, Object>) item1.get().get("product")).get("productName"));
        assertEquals(buildCategory1().getCategoryName(), ((Map<String, Object>) ((Map<String, Object>) item1.get().get("product")).get("category")).get("categoryName"));

        assertEquals(expectedItem2.getUnitPrice(), item2.get().get("unitPrice"));
        assertEquals(expectedItem2.getQuantity(), item2.get().get("quantity"));
        assertEquals(expectedItem2.getUnitPrice() * expectedItem2.getQuantity() * (1 - expectedItem2.getDiscount()), item2.get().get("price"));
        assertEquals(expectedItem2.getDiscount(), item2.get().get("discount"));
        assertEquals(buildProduct2(null).getProductName(), item2.get().get("productName"));
        assertEquals(category3Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) item2.get().get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildCategory3().getCategoryName(), ((Map<String, Object>) item2.get().get("category")).get("categoryName"));
        assertEquals(category3Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) ((Map<String, Object>) item2.get().get("product")).get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildProduct2(null).getProductName(), ((Map<String, Object>) item2.get().get("product")).get("productName"));
        assertEquals(buildCategory3().getCategoryName(), ((Map<String, Object>) ((Map<String, Object>) item2.get().get("product")).get("category")).get("categoryName"));

        assertEquals(expectedItem3.getUnitPrice(), item3.get().get("unitPrice"));
        assertEquals(expectedItem3.getQuantity(), item3.get().get("quantity"));
        assertEquals(expectedItem3.getUnitPrice() * expectedItem3.getQuantity() * (1 - expectedItem3.getDiscount()), item3.get().get("price"));
        assertEquals(expectedItem3.getDiscount(), item3.get().get("discount"));
        assertEquals(buildProduct3(null).getProductName(), item3.get().get("productName"));
        assertEquals(category1Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) item3.get().get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildCategory1().getCategoryName(), ((Map<String, Object>) item3.get().get("category")).get("categoryName"));
        assertEquals(category1Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) ((Map<String, Object>) item3.get().get("product")).get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildProduct3(null).getProductName(), ((Map<String, Object>) item3.get().get("product")).get("productName"));
        assertEquals(buildCategory1().getCategoryName(), ((Map<String, Object>) ((Map<String, Object>) item3.get().get("product")).get("category")).get("categoryName"));

        assertEquals(expectedItem4.getUnitPrice(), item4.get().get("unitPrice"));
        assertEquals(expectedItem4.getQuantity(), item4.get().get("quantity"));
        assertEquals(expectedItem4.getUnitPrice() * expectedItem4.getQuantity() * (1 - expectedItem4.getDiscount()), item4.get().get("price"));
        assertEquals(expectedItem4.getDiscount(), item4.get().get("discount"));
        assertEquals(buildProduct3(null).getProductName(), item4.get().get("productName"));
        assertEquals(category1Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) item4.get().get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildCategory1().getCategoryName(), ((Map<String, Object>) item4.get().get("category")).get("categoryName"));
        assertEquals(category1Ofcategories.get().get(daoFixture.getUuid().getName()), ((Map<String, Object>) ((Map<String, Object>) item4.get().get("product")).get("category")).get(daoFixture.getUuid().getName()));
        assertEquals(buildProduct3(null).getProductName(), ((Map<String, Object>) item4.get().get("product")).get("productName"));
        assertEquals(buildCategory1().getCategoryName(), ((Map<String, Object>) ((Map<String, Object>) item4.get().get("product")).get("category")).get("categoryName"));

        assertTrue((boolean) orderInfo.get("hasHeavyItem"));
        assertEquals(orderInfo.get("numberOfCategories"), categories.size());
        assertEquals(orderInfo.get("numberOfDiscountedItemsOutOfStock"), discountedItemsOutOfStock.size());
        assertEquals(orderInfo.get("numberOfItems"), items.size());
        assertFalse((boolean) orderInfo.get("shipped"));

        final double expectedTotalPrice = new BigDecimal(item1.get().get("price").toString())
                .add(new BigDecimal(item2.get().get("price").toString()))
                .add(new BigDecimal(item3.get().get("price").toString()))
                .add(new BigDecimal(item4.get().get("price").toString())).doubleValue();
        final double expectedTotalWeight = new BigDecimal(item1.get().get("weight").toString())
                .add(new BigDecimal(item2.get().get("weight").toString()))
                .add(new BigDecimal(item3.get().get("weight").toString()))
                .add(new BigDecimal(item4.get().get("weight").toString())).doubleValue();

        assertEquals(orderInfo.get("totalPrice"), expectedTotalPrice);
        assertEquals(orderInfo.get("totalWeight"), expectedTotalWeight);

        final UUID item1Id = (UUID) item1.get().get(daoFixture.getUuid().getName());
        final EClass _orderItemQuery = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_ORDER_ITEM).get();
        final EReference _orderItemCategory = _orderItemQuery.getEAllReferences().stream().filter(r -> "category".equals(r.getName())).findAny().get();

        final long startTs2 = System.currentTimeMillis();
        final Collection<? extends Map<String, Object>> results2 = daoFixture.getDao().getNavigationResultAt(item1Id, _orderItemCategory);
        final long endTs2 = System.currentTimeMillis();
        log.debug("Results returned in {} ms:\n{}", (endTs2 - startTs2), results2);

        final UUID category1Id = (UUID) category1Ofcategories.get().get(daoFixture.getUuid().getName());
        final EClass _categoryInfo = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_CATEGORY_INFO).get();
        final EReference _products = _categoryInfo.getEAllReferences().stream().filter(r -> "products".equals(r.getName())).findAny().get();

        final long startTs3 = System.currentTimeMillis();
        final Collection<? extends Map<String, Object>> results3 = daoFixture.getDao().getNavigationResultAt(category1Id, _products);
        final long endTs3 = System.currentTimeMillis();
        log.debug("Results returned in {} ms:\n{}", (endTs3 - startTs3), results3);

        final Optional<? extends Map<String, Object>> product3Ofcategory1 = results3.stream().filter(c -> Objects.equals(c.get("productName"), buildProduct3(null).getProductName())).findAny();

        final UUID product3Id = (UUID) product3Ofcategory1.get().get(daoFixture.getUuid().getName());
        final EClass _productInfo = daoFixture.getAsmUtils().getClassByFQName(DEMO_SERVICE_PRODUCT_INFO).get();
        final EReference _category = _productInfo.getEAllReferences().stream().filter(r -> "category".equals(r.getName())).findAny().get();

        final long startTs4 = System.currentTimeMillis();
        final Collection<? extends Map<String, Object>> results4 = daoFixture.getDao().getNavigationResultAt(product3Id, _category);
        final long endTs4 = System.currentTimeMillis();
        log.debug("Results returned in {} ms:\n{}", (endTs4 - startTs4), results4);

        assertEquals(buildCategory1().getCategoryName(), results4.iterator().next().get("categoryName"));
        assertEquals(category1Id, results4.iterator().next().get(daoFixture.getUuid().getName()));

        OrderInfo.OrderInfoDao orderInfoDao = new internal.demo.services.OrderInfoDaoImpl();
        ((internal.demo.services.OrderInfoDaoImpl) orderInfoDao).setAsmModel(daoFixture.getAsmModel());
        ((internal.demo.services.OrderInfoDaoImpl) orderInfoDao).setDao(daoFixture.getDao());
        List<sdk.demo.services.OrderInfo> daoResult = orderInfoDao.search()
                .orderBy(sdk.demo.services.OrderInfo.Attribute.ORDER_DATE)
                .orderBy(sdk.demo.services.OrderInfo.Attribute.TOTAL_WEIGHT)
                .limit(10)
                .execute();
        assertEquals(daoResult.size(), 1);
        assertEquals(daoResult.get(0).get__identifier(), id);

        final List<sdk.demo.services.CategoryInfo> daoCategories1 = orderInfoDao.searchCategories(daoResult.get(0).get__identifier())
                .filterByCategoryName(matches("[Cc]ategory-[0-9]+"))
                .orderByDescending(sdk.demo.services.CategoryInfo.Attribute.CATEGORY_NAME)
                .limit(1)
                .execute();
        assertEquals(1, daoCategories1.size());
        assertEquals(buildCategory3().getCategoryName(), daoCategories1.get(0).getCategoryName());
        final List<sdk.demo.services.CategoryInfo> daoCategories2 = orderInfoDao.searchCategories(daoResult.get(0).get__identifier())
                .filterBy(THIS_NAME + ".categoryName!like('Cat%')")
                .orderBy(sdk.demo.services.CategoryInfo.Attribute.CATEGORY_NAME)
                .maskedBy(sdk.demo.services.CategoryInfo.CategoryInfoDao.Mask.categoryInfoMask()
                        .withCategoryName())
                .limit(1)
                .execute();
        assertEquals(1, daoCategories2.size());
        assertEquals(buildCategory1().getCategoryName(), daoCategories2.get(0).getCategoryName());

        cleanup(daoFixture, Collections.singleton(id));
    }

    private static CategoryInfo buildCategory1() {
        return CategoryInfo.builder().withCategoryName("Category-1").build();
    }

    private static CategoryInfo buildCategory2() {
        return CategoryInfo.builder().withCategoryName("Category-2").build();
    }

    private static CategoryInfo buildCategory3() {
        return CategoryInfo.builder().withCategoryName("Category-3").build();
    }

    private static ProductInfo buildProduct1(final CategoryInfo categoryInfo) {
        return ProductInfo.builder()
                .withProductName("Product-1")
                .withUnitPrice(1.11d)
                .withWeight(0.7d)
                .withUnitsInStock(10)
//                .discounted(true)
                .withCategory(categoryInfo)
                .build();
    }

    private static ProductInfo buildProduct2(final CategoryInfo categoryInfo) {
        return ProductInfo.builder()
                .withProductName("Product-2")
                .withUnitsInStock(0)
                .withUnitPrice(2.22d)
//                .discounted(false)
                .withWeight(0.1d)
                .withCategory(categoryInfo)
                .build();
    }

    private static ProductInfo buildProduct3(final CategoryInfo categoryInfo) {
        return ProductInfo.builder()
                .withProductName("Product-3")
                .withUnitPrice(3.33d)
//                .discounted(true)
                .withWeight(2.3d)
                .withCategory(categoryInfo)
                .build();
    }

    private static ShipperInfo buildShipper() {
        return ShipperInfo.builder()
                .withCompanyName("CompanyName")
                .withLocation(Gps.builder().latitude(47.5107529).longitude(19.0369182).build())
                .build();
    }

    private static InternationalOrderInfo buildInternationalOrder(final ShipperInfo shipperInfo, final ProductInfo product1, final ProductInfo product2, final ProductInfo product3) {
        return InternationalOrderInfo.builder()
                .withShipper(shipperInfo)
                .withCustomsDescription("customs")
                .withExciseTax(0.05d)
                .withItems(ImmutableList.of(
                        OrderItem.builder()
                                .withProduct(product1)
                                .withUnitPrice(Double.valueOf(12.0))
                                .withQuantity(11)
                                .withDiscount(Double.valueOf(0.1))
                                .build(),
                        OrderItem.builder()
                                .withProduct(product2)
                                .withUnitPrice(Double.valueOf(15.0))
                                .withQuantity(21)
                                .withDiscount(Double.valueOf(0.05))
                                .build(),
                        OrderItem.builder()
                                .withProduct(product3)
                                .withUnitPrice(Double.valueOf(25.0))
                                .withQuantity(6)
                                .withDiscount(Double.valueOf(0))
                                .build(),
                        OrderItem.builder()
                                .withProduct(product3)
                                .withUnitPrice(Double.valueOf(25.0))
                                .withQuantity(2)
                                .withDiscount(Double.valueOf(0))
                                .build()
                        )
                )
                .withOrderDate(OffsetDateTime.of(2019, 10, 4, 15, 55, 12, 123000000, ZoneOffset.of("-05:00")))
                //.withOrderDate(ZonedDateTime.of(2019, 10, 4, 15, 55, 12, 123000000, ZoneId.of("Europe/Budapest")))  // non-UTC zoned timestamps are not supported yet
                .withPriority(Priority.URGENT)
                .build();
    }
}