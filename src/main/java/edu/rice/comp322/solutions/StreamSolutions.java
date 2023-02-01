package edu.rice.comp322.solutions;

import edu.rice.comp322.provided.streams.models.Customer;
import edu.rice.comp322.provided.streams.models.Order;
import edu.rice.comp322.provided.streams.models.Product;
import edu.rice.comp322.provided.streams.provided.DiscountObject;
import edu.rice.comp322.provided.streams.repos.CustomerRepo;
import edu.rice.comp322.provided.streams.repos.OrderRepo;
import edu.rice.comp322.provided.streams.repos.ProductRepo;

import java.util.*;

import java.time.LocalDate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class containing all of the students implemented solutions to the stream problems.
 */
public class StreamSolutions {

    /**
     * Use this function with the appropriate streams solution test to ensure repos load correctly.
     */
    public static List<Long> problemZeroSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        List<Long> counts = new ArrayList<>();
        counts.add(customerRepo.findAll().stream().count());
        counts.add(orderRepo.findAll().stream().count());
        counts.add(productRepo.findAll().stream().count());

        return counts;
    }

    /**
     * Use this function with the appropriate streams solution test to ensure repos load correctly.
     */
    public static List<Long> problemZeroPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        List<Long> counts = new ArrayList<>();
        counts.add(customerRepo.findAll().parallelStream().count());
        counts.add(orderRepo.findAll().parallelStream().count());
        counts.add(productRepo.findAll().parallelStream().count());

        return counts;
    }

    // TODO: Implement problem one using sequential streams

    /**
     * Calculate the companies maximum possible revenue from all online sales during the month of February
     * using sequential functional solution.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return the maximum possible revenue of type Double
     */
    public static Double problemOneSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Stream<Double> sums = orderRepo.findAll().stream()
                .filter(order -> order.getOrderDate().getMonthValue() == 2)
                .map(order -> order.getProducts().stream().map(Product::getFullPrice).reduce(0.0, Double::sum));

        var total = sums.reduce(0.0, Double::sum);
        return total;
    }

    // TODO: Implement problem one using parallel streams
    /**
     * Calculate the companies maximum possible revenue from all online sales during the month of February
     * using sequential functional solution.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return the maximum possible revenue of type Double
     */
    public static Double problemOnePar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {

        Stream<Double> sums = orderRepo.findAll().parallelStream().filter(order -> order.getOrderDate().getMonthValue() == 2)
                .map(order -> order.getProducts().stream().map(Product::getFullPrice).reduce(0.0, Double::sum));

        var total = sums.reduce(0.0, Double::sum);
        return total;
    }

    // TODO: Implement problem two using sequential streams
    /**
     * Get the order IDs of the 5 most recently placed orders sequentially.
     *  @param customerRepo a JAVA collection of Customer information
     *  @param orderRepo a JAVA collection of Order information
     *  @param productRepo a JAVA collection of Product information
     * @return a Set of type Long containing order IDs of the 5 most recently placed orders sequentially
     */
    public static Set<Long> problemTwoSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Set<Long> sorted = orderRepo.findAll().stream().sorted(Comparator.comparing(Order::getOrderDate).reversed())
                .map(order -> order.getId()).limit(5).collect(Collectors.toSet());
        return sorted;
    }

    // TODO: Implement problem two using parallel streams
    /**
     * Get the order IDs of the 5 most recently placed orders sequentially.
     *  @param customerRepo a JAVA collection of Customer information
     *  @param orderRepo a JAVA collection of Order information
     *  @param productRepo a JAVA collection of Product information
     * @return a Set of type Long containing order IDs of the 5 most recently placed orders sequentially
     */
    public static Set<Long> problemTwoPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        Set<Long> sorted = orderRepo.findAll().parallelStream()
                .sorted(Comparator.comparing(Order::getOrderDate).reversed())
                .map(order -> order.getId())
                .limit(5).collect(Collectors.toSet());
        return sorted;
    }

    // TODO: Implement problem three using sequential streams

    /**
     * Count the number of distinct customers making purchases sequentially.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a value of type Long, representing the number of distinct customers making purchases
     */
    public static Long problemThreeSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var distinctNum = orderRepo.findAll().stream().map(order -> order.getCustomer()).distinct().count();

        return distinctNum;
    }

    // TODO: Implement problem three using parallel streams

    /**
     * Count the number of distinct customers making purchases parallely.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a value of type Long, representing the number of distinct customers making purchases
     */
    public static Long problemThreePar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var distinctNum = orderRepo.findAll().parallelStream().map(order -> order.getCustomer()).distinct().count();

        return distinctNum;
    }

    // TODO: Implement problem four using sequential streams

    /**
     * Calculate the total discount for all orders placed in March 2021 sequentially.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a value of type Double representing the total discount for all orders placed
     *              in March 2021
     */
    public static Double problemFourSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var totalDiscount = orderRepo.findAll().stream()
                .filter(order -> (order.getOrderDate().getMonthValue() == 3 && order.getOrderDate().getYear() == 2021))
                .map(order -> order.getProducts().stream()
                        .map(product-> new DiscountObject(order.getCustomer(), product).getDiscount())
                        .reduce(0.0, Double::sum ))
                .reduce(0.0, Double::sum);

        return totalDiscount;
    }

    // TODO: Implement problem four using parallel streams
    /**
     * Calculate the total discount for all orders placed in March 2021 parallely.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a value of type Double representing the total discount for all orders placed
     *              in March 2021
     */
    public static Double problemFourPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var totalDiscount = orderRepo.findAll().parallelStream()
                .filter(order -> (order.getOrderDate().getMonthValue() == 3 && order.getOrderDate().getYear() == 2021))
                .map(order -> order.getProducts().stream()
                        .map(product-> new DiscountObject(order.getCustomer(), product).getDiscount())
                        .reduce(0.0, Double::sum ))
                .reduce(0.0, Double::sum);

        return totalDiscount;
    }
    //DATA MAP CREATION
    // TODO: Implement problem five using sequential streams

    /**
     * Sequentially create a mapping between customers IDs and the total dollar amount they spent on products,
     * assuming they purchased all products at full price.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a mapping which key is of type Long representing customer IDs
     *              and value is of type Double representing the customer's total dollar amount they spent on products
     */
    public static Map<Long, Double> problemFiveSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var result = orderRepo.findAll().stream()
                .collect(Collectors.groupingBy(order->order.getCustomer()
                        .getId(), Collectors.summingDouble(order->order.getProducts().stream()
                        .map(product -> product.getFullPrice()).reduce(0.0, Double::sum))));

        return result;
    }

    // TODO: Implement problem five using parallel streams
    /**
     * Parallely create a mapping between customers IDs and the total dollar amount they spent on products,
     * assuming they purchased all products at full price.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type Long representing customer IDs
     *              and value is of type Double representing the customer's total dollar amount they spent on products
     */
    public static Map<Long, Double> problemFivePar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var customerMap = orderRepo.findAll().parallelStream()
                .collect(Collectors.groupingBy(order->order.getCustomer()
                        .getId(), Collectors.summingDouble(order->order.getProducts().stream()
                        .map(product -> product.getFullPrice()).reduce(0.0, Double::sum))));

        return customerMap;
    }

    // TODO: Implement problem six using sequential streams

    /**
     * Sequentially create a mapping between product categories and the average cost of an item in that category.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type String representing the product category
     *              and value is of type Double representing average cost of an item in that category
     */
    public static Map<String, Double> problemSixSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var productMap = productRepo.findAll().stream().collect(Collectors
                .groupingBy(product -> product.getCategory(), Collectors.averagingDouble(product->product.getFullPrice())));
        return productMap;
    }

    // TODO: Implement problem six using parallel streams
    /**
     * Parallely create a mapping between product categories and the average cost of an item in that category.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type String representing the product category
     *              and value is of type Double representing average cost of an item in that category
     */
    public static Map<String, Double> problemSixPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var productMap = productRepo.findAll().parallelStream().collect(Collectors
                .groupingBy(product -> product.getCategory(), Collectors.averagingDouble(product->product.getFullPrice())));
        return productMap;
    }

    // TODO: Implement problem seven using sequential streams

    /**
     * Sequentially create a mapping between products IDs in the tech category (category = "Tech") and the IDs of the
     * customers which ordered them.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type Long representing products IDs in the tech category
     *              and value is a type set of type Long representing the IDs of the customers which ordered them
     */
    public static Map<Long, Set<Long>> problemSevenSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var techOrders = productRepo.findAll().stream()
                .filter(product -> product.getCategory().equals("Tech"))
                .collect(Collectors.groupingBy(product->product.getId(),Collectors.flatMapping(product -> product.getOrders()
                                        .stream().map(order -> order.getCustomer().getId()), Collectors.toSet())));

        return techOrders;
    }

    // TODO: Implement problem seven using parallel streams
    /**
     * Parallely create a mapping between products IDs in the tech category (category = "Tech") and the IDs of the
     * customers which ordered them.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type Long representing products IDs in the tech category
     *              and value is a type set of type Long representing the IDs of the customers which ordered them
     */
    public static Map<Long, Set<Long>> problemSevenPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var techOrders = productRepo.findAll().parallelStream().filter(product -> product
                        .getCategory().equals("Tech")).collect(Collectors.groupingBy(product->product
                .getId(),Collectors.flatMapping(product -> product.getOrders().stream()
                                        .map(order -> order.getCustomer().getId()), Collectors.toSet())));
        return techOrders;
    }

    // TODO: Implement problem eight using sequential streams

    /**
     * Sequentially create a mapping between the IDs of customers without membership tiers and their sales utilization
     * rate.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type Long representing IDs of customers without membership tiers
     *              and value is a type Double representing the customer's sales utilization rate
     */
    public static Map<Long, Double> problemEightSeq(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var customerUtilizRate = orderRepo.findAll().stream()
                .filter(order -> order.getCustomer().getTier() == 0)
                .collect(Collectors.groupingBy(order -> order.getCustomer().getId(),
                        Collectors.flatMapping(order-> order.getProducts().stream()
                                        .map(product -> new DiscountObject(order.getCustomer(), product))
                                        .map(discount->discount.getDiscount() == 0.0 ? 0.0 : 1.0), Collectors
                                .averagingDouble(number-> number))));
        return customerUtilizRate;
    }


    // TODO: Implement problem eight using parallel streams
    /**
     * Parallely create a mapping between the IDs of customers without membership tiers and their sales utilization
     * rate.
     * @param customerRepo a JAVA collection of Customer information
     * @param orderRepo a JAVA collection of Order information
     * @param productRepo a JAVA collection of Product information
     * @return a map which key is of type Long representing IDs of customers without membership tiers
     *              and value is a type Double representing the customer's sales utilization rate
     */
    public static Map<Long, Double> problemEightPar(CustomerRepo customerRepo, OrderRepo orderRepo, ProductRepo productRepo) {
        var customerUtilizRate = orderRepo.findAll().parallelStream()
                .filter(order -> order.getCustomer().getTier() == 0)
                .collect(Collectors.groupingBy(order -> order.getCustomer().getId(),
                        Collectors.flatMapping(order -> order.getProducts().stream()
                                .map(product -> new DiscountObject(order.getCustomer(), product))
                                .map(disc -> disc.getDiscount() == 0.0 ? 0.0 : 1.0), Collectors.averagingDouble(num -> num))));
        return customerUtilizRate;
    }
}

