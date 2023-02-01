package edu.rice.comp322.solutions;

import edu.rice.comp322.provided.trees.GList;
import edu.rice.comp322.provided.trees.Tree;

public class TreeSolutions {

    // @TODO Implement tree sum

    /**
     * Recursively sum all the values of all the nodes in a Tree without using
     * any higher order functions, mutation or loops.
     * @param tree the tree to sum
     * @return sum of all the values of all the nodes in the given tree
     */
    public static Integer problemOne(Tree<Integer> tree) {
        if (tree.children().isEmpty()) {
            return tree.value();
        } else {
            return tree.value() + problemOne(tree.children().head()) + problemOne(Tree.makeNode(0, tree.children().tail()));
        }
    }


    // @TODO Implement tree sum using higher order list functions

    /**
     * Calculate the sum all the values of all the nodes in a Tree
     * using the higher order GList functions map, fold, and Filter.
     * @param tree, the tree to sum
     * @return sum of all the values of all the nodes in the given tree
     */
    public static Integer problemTwo(Tree<Integer> tree) {
        return tree.value() + tree.children().fold(0, (x, childValue) -> x + problemTwo(childValue));

    }

    /*
     * Problem 3's solution should be written in the Tree.java class at line 118.
     */

    // @TODO Calculate the sum of the elements of the tree using tree fold

    /**
     * Calculate the sum of the elements of the tree using tree fold.
     * @param tree, the tree to sum
     * @return sum of all the values of all the nodes in the given tree
     */
    public static Integer problemFour(Tree<Integer> tree) {
        return tree.value() + tree.children().fold(0, (x, childValue) -> x + problemTwo(childValue));
    }

}
