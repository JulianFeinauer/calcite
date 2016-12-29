package org.apache.calcite.test;

import org.apache.calcite.adapter.csv.CsvTranslatableTable;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Created by julian on 25.12.16.
 */
public class JulianTest {

    private static SchemaPlus rootSchema;

    @Test
    public void test() throws Exception {

        final RelDataType relDataType = Mockito.mock(RelDataType.class);

        rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("Table1", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return relDataType;
//                return typeFactory.builder()
//                        .add("A", typeFactory.createJavaType(Double.class))
//                        .add("C", SqlTypeName.INTEGER).build();
            }
        });
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs(Arrays.asList((RelTraitDef)ConventionTraitDef.INSTANCE))
                .programs(Programs.CALC_PROGRAM).build();

//        RexBuilder rexBuilder = new RexBuilder()
//        RelOptPlanner planner = new VolcanoPlanner();
//        RelOptCluster cluster = RelOptCluster.create(planner, )

        final RelBuilder builder = RelBuilder.create(config);

        final List<RelDataTypeField> relDataTypeFields = Arrays.asList(
                (RelDataTypeField) new RelDataTypeFieldImpl("A", 0, builder.getTypeFactory().createJavaType(Double.class)),
                (RelDataTypeField) new RelDataTypeFieldImpl("C", 1, builder.getTypeFactory().createJavaType(Double.class))
        );

        when(relDataType.getFieldList()).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                // System.out.println("Hallo :)");
                return relDataTypeFields;
            }
        });
        when(relDataType.getFieldCount()).thenReturn(
                relDataTypeFields.size());
        when(relDataType.getFieldNames()).thenReturn(
                Arrays.asList("A","C"));

        RelNode node = null;
        try {
            node = builder
                    .scan("Table1")
                    .filter(
                            builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("C"), builder.literal(10)))
                    .project(
                            builder.alias(builder.call(SqlStdOperatorTable.CEIL, builder.field("C"), builder.field("A")), "B"),
                            builder.alias(builder.call(SqlStdOperatorTable.CEIL, builder.field("C"), builder.field("A")), "D")
                    )
                    .build();
        } catch (Exception e) {
            // do nothing
            e.printStackTrace();
        }

        System.out.println(RelOptUtil.toString(node));

        // Go to other calling convention?

        System.out.println(node.getConvention());

        RelNode run = Programs.ofRules(TestRule1.INSTANCE, TestRule2.INSTANCE, TestRule3.INSTANCE, TestRule4.INSTANCE).run(node.getCluster().getPlanner(), node, node.getCluster().getPlanner().emptyTraitSet().replace(JulianRel.CONVENTION).replace(EnumerableConvention.INSTANCE));

        System.out.println(RelOptUtil.toString(run));

        RelRoot root = RelRoot.of(run, SqlKind.SELECT);

        Bindable bindable = EnumerableInterpretable.toBindable(Collections.EMPTY_MAP, CalcitePrepareImpl.Dummy.getSparkHandler(false), (EnumerableRel) root.rel, EnumerableRel.Prefer.ANY);

        Enumerable bind = bindable.bind(null);

        assertEquals(1, bind.count());
        assertEquals("Hallo", bind.toList().get(0));
    }

    public static interface JulianRel extends RelNode {

        void implement(Implementor implementor);

        public static Convention CONVENTION = new Convention.Impl("JULIANCON", JulianRel.class);

        class Implementor {
            final List<Pair<String, String>> list =
                    new ArrayList<Pair<String, String>>();

            Table table;

            public void add(String findOp, String aggOp) {
                list.add(Pair.of(findOp, aggOp));
            }

            public void visitChild(int ordinal, RelNode input) {
                assert ordinal == 0;
                ((JulianRel)input).implement(this);
            }
        }

    }

    public static class JulianFilter extends Filter implements JulianRel {

        protected JulianFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
            super(cluster, traits, child, condition);
        }

        @Override
        public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
            return new JulianFilter(getCluster(), traitSet, input, condition);
        }

        @Override
        public void implement(Implementor implementor) {
            implementor.visitChild(0, getInput());
            System.out.println("Visit Filter");
        }
    }

    public static class JulianProject extends Project implements JulianRel {

        protected JulianProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
            super(cluster, traits, input, projects, rowType);
        }

        @Override
        public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
            return new JulianProject(getCluster(), traitSet, input, projects, rowType);
        }


        @Override
        public void implement(Implementor implementor) {
            implementor.visitChild(0, getInput());
            System.out.println("Visit Project");
        }
    }

    public static class JulianTableScan extends TableScan implements JulianRel {

        protected JulianTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, table);
        }

        @Override
        public void implement(Implementor implementor) {
            System.out.println("Visit scan");
            implementor.table = rootSchema.getTable("Table1");
        }
    }

    public static class JulianToEnumerableConverter extends ConverterImpl implements EnumerableRel {

        /**
         * Creates a ConverterImpl.
         *
         * @param cluster  planner's cluster
         * @param traitDef the RelTraitDef this converter converts
         * @param traits   the output traits of this converter
         * @param child    child rel (provides input traits)
         */
        protected JulianToEnumerableConverter(RelOptCluster cluster, RelTraitDef traitDef, RelTraitSet traits, RelNode child) {
            super(cluster, traitDef, traits, child);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new JulianToEnumerableConverter(getCluster(), ConventionTraitDef.INSTANCE, traitSet, sole(inputs));
        }

        @Override
        public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
            JulianRel.Implementor implementor1 = new JulianRel.Implementor();
            implementor1.visitChild( 0, getInput());
            final BlockBuilder list = new BlockBuilder();

            final RelDataType rowType = getRowType();
            final PhysType physType =
                    PhysTypeImpl.of(
                            implementor.getTypeFactory(), rowType,
                            pref.prefer(JavaRowFormat.ARRAY));

            try {
                Constructor constructor = ArrayList.class.getConstructor();

                Expression empty_list = list.append("empty_list",
                        Expressions.new_(constructor));

                Expression hallo = Expressions.constant("Hallo");

                list.append(
                        Expressions.call(empty_list,
                                ArrayList.class.getMethod("add", Object.class),
                                hallo));

                Expression empty = list.append("empty_enumerable",
                        Expressions.call(Linq4j.class, "asEnumerable", empty_list));

                list.add(Expressions.return_(null, empty));
                return implementor.result(physType, list.toBlock());
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class TestRule extends RelOptRule {

        public static RelOptRule INSTANCE = new TestRule();

        private TestRule() {
                super(operand(LogicalFilter.class, operand(LogicalTableScan.class, none())),
                        "MyFirstFilterRule");
            }

        @Override
        public void onMatch(RelOptRuleCall call) {
            System.out.println("Match :)");
            RelNode rel = call.rel(0);
            call.transformTo(new JulianFilter(rel.getCluster(), rel.getTraitSet().replace(JulianRel.CONVENTION), rel.getInput(0),((Filter)rel).getCondition()));
        }
    }

    public static class TestRule1 extends ConverterRule {

        public static RelOptRule INSTANCE = new TestRule1();

        public TestRule1() {
            super(LogicalFilter.class, Convention.NONE, JulianRel.CONVENTION, "FilterConversion");
        }

        @Override
        public RelNode convert(RelNode rel) {
            return new JulianFilter(rel.getCluster(), rel.getTraitSet().replace(JulianRel.CONVENTION),
                    convert(((LogicalFilter)rel).getInput(), JulianRel.CONVENTION),((Filter)rel).getCondition());
        }

    }

    public static class TestRule2 extends ConverterRule {

        public static RelOptRule INSTANCE = new TestRule2();

        public TestRule2() {
            super(LogicalProject.class, Convention.NONE, JulianRel.CONVENTION, "ProjectConversion");
        }

        @Override
        public RelNode convert(RelNode rel) {
            return new JulianProject(rel.getCluster(), rel.getTraitSet().replace(JulianRel.CONVENTION), convert(((LogicalProject)rel).getInput(), JulianRel.CONVENTION), ((Project)rel).getProjects(), rel.getRowType());
        }

    }

    public static class TestRule3 extends ConverterRule {

        public static RelOptRule INSTANCE = new TestRule3();

        public TestRule3() {
            super(LogicalTableScan.class, Convention.NONE, JulianRel.CONVENTION, "ScanConversion");
        }

        @Override
        public RelNode convert(RelNode rel) {
            return new JulianTableScan(rel.getCluster(), rel.getTraitSet().replace(JulianRel.CONVENTION), ((TableScan)rel).getTable());
        }

    }

    public static class TestRule4 extends ConverterRule {

        public static RelOptRule INSTANCE = new TestRule4();

        public TestRule4() {
            super(RelNode.class, JulianRel.CONVENTION, EnumerableConvention.INSTANCE, "JulianToEnumerable");
        }

        @Override
        public RelNode convert(RelNode rel) {
            System.out.println(rel);
            return new JulianToEnumerableConverter(rel.getCluster(), ConventionTraitDef.INSTANCE, rel.getTraitSet().replace(EnumerableConvention.INSTANCE), rel);
        }

    }

}
