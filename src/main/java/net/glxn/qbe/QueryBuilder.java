package net.glxn.qbe;

import net.glxn.qbe.exception.*;
import net.glxn.qbe.types.*;

import org.slf4j.*;

import javax.persistence.*;
import javax.persistence.criteria.*;

import java.lang.reflect.*;
import java.util.*;

import static java.lang.String.*;
import static net.glxn.qbe.reflection.Reflection.*;

public class QueryBuilder<T, E> {
	static HashMap<String, HashMap> cache = new HashMap();

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final Class<E> entityClass;
	// private final E entity;
	private final T example;
	// private HashMap<String, Field> entityColumnFields = new HashMap<String,
	// Field>();
	// private HashMap<String, String> entityColumnName = new HashMap<String,
	// String>();
	private HashMap<String, Field> entityColumnFields = null;
	private HashMap<String, String> entityColumnName = null;
	// private HashMap<String, Field> entityIdFields;
	private HashMap<String, Object> exampleFields = new HashMap<String, Object>();
	List<Predicate> criteria = new ArrayList<Predicate>();

	private final EntityManager entityManager;
	private CriteriaBuilder cb;
	private CriteriaQuery<E> criteriaQuery;
	private Root<E> root;
	private boolean includeNonString = false;

	private final LinkedList<QBEOrder> ordering = new LinkedList<QBEOrder>();
	Matching matching;
	Junction junction;
	Case caseQuery;

	QueryBuilderT49(T example, Class<E> entityClass, EntityManager entityManager, Matching matching, Junction junction,
			Case caseQuery) {
		this.example = example;
		this.entityClass = entityClass;
		this.entityManager = entityManager;
		this.matching = matching;
		this.junction = junction;
		this.caseQuery = caseQuery;

		if (entityColumnFields == null) {
			entityColumnFields = cache.get(entityClass.getName() + ":Field");
			if (entityColumnFields == null) {
				entityColumnFields = new HashMap<String, Field>();
				cache.put(entityClass.getName() + ":Field", entityColumnFields);
			}
		}
		if (entityColumnName == null) {
			entityColumnName = cache.get(entityClass.getName() + ":Name");
			if (entityColumnName == null) {
				entityColumnName = new HashMap<String, String>();
				cache.put(entityClass.getName() + ":Name", entityColumnName);
			}
		}

		buildColumnFields(entityClass, null);
	}

	// private HashMap<String, Field> fieldMapForEntity(Class<E> entityClass) {
	private void buildColumnFields(Class clazz, String prefix) {
		if (clazz == null || clazz == Object.class)
			return;
		if (entityColumnFields.size() > 0) {
			return;
		}
		if (prefix == null)
			prefix = "";
		// HashMap<String, Field> entityFields = new HashMap<String, Field>();

		for (Field field : clazz.getDeclaredFields()) {
			System.out.println("buildColumnFields:" + field.getName());
			if (Modifier.isFinal(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
				continue;
			}
			// EmbeddedId ann = field.getAnnotation(EmbeddedId.class);
			// Embedded ann = field.getAnnotation(Embedded.class);
			// if (ann != null) {
			Column column = field.getAnnotation(Column.class);
			if (column != null) {
				entityColumnFields.put(field.getName(), field);
				entityColumnName.put(field.getName(), prefix + field.getName());
				continue;
			}
			EmbeddedId ann = field.getAnnotation(EmbeddedId.class);
			if (ann != null) {
				buildColumnFields(field.getType(), prefix + field.getName() + ".");
				continue;
			}
			Embedded embedded = field.getAnnotation(Embedded.class);
			if (embedded != null) {
				buildColumnFields(field.getType(), prefix + field.getName() + ".");
				continue;
			}
		}
		return;
	}

	private void buildExampleFiled(Object example, String prefix) {
		if (example == null)
			return;
		if (prefix == null)
			prefix = "";

		Class clazz = example.getClass();
		System.out.println("Strat:" + prefix + ":" + clazz.getName());
		if (clazz == null || clazz == Object.class)
			return;

		//////////////
		// System.out.println("entityFields:" +
		////////////// Arrays.toString(entityColumnFields.keySet().toArray()));
		for (Field field : clazz.getDeclaredFields()) {
			System.out.println("buildExampleFiled:" + field.getName());
			if (Modifier.isFinal(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
				continue;
			}
			field.setAccessible(true);
			Object value = null;
			try {
				value = field.get(example);
			} catch (IllegalAccessException e) {
				Object[] args = { field.getName(), example.getClass(), e };
				System.out.println(String.format("FAILED TO ACCESS FIELD [%s] ON CLASS [%s]. Cause: %s", args));
			}
			if (value == null)
				continue;
			String name;
			if ((name = entityColumnName.get(field.getName())) != null) {
				exampleFields.put(field.getName(), value);
			} else {
				buildExampleFiled(value, prefix + field.getName() + ".");
			}

		}
		System.out.println("ALL FIELD:" + Arrays.toString(exampleFields.keySet().toArray()));
		return;
	}

	public void orderBy(String orderBy, net.glxn.qbe.types.Order order) {
		Field field = null;
		String nameOfFieldToOrderBy = null;
		System.out.println("ORDERBY:" + orderBy);
		String tokens[] = orderBy.split("\\.");
		orderBy = tokens[tokens.length - 1];

		String nameId = entityColumnName.get(tokens[tokens.length - 1]);
		if (nameId != null) {
			nameOfFieldToOrderBy = nameId;
		}

		if (nameOfFieldToOrderBy == null) {
			String message = "" + "Unable to create order parameters for the supplied order by argument [" + orderBy
					+ "] " + "You must use on of the following: " + "name property of the "
					+ Column.class.getCanonicalName() + " annotation "
					+ "or the name of the field on the class you are querying ";
			throw new OrderCreationException(message);
		}

		ordering.add(new QBEOrder(nameOfFieldToOrderBy, order));
	}

	TypedQuery<E> build(boolean includeNonString) {
		if (this.includeNonString != includeNonString || (cb == null && root == null)) {
			createCriteriaQueryAndRoot();
			// entityColumnFields = buildColumnFields(entityClass, null);
			this.includeNonString = includeNonString;
			buildExampleFiled(example, "");
		}
		buildCriteria();
		addOrderByToCriteria();
		criteriaQuery.select(root);
		TypedQuery<E> retult = createQueryAndSetParameters();
		return retult;
	}

	public TypedQuery buildCount(boolean includeNonString) {
		if (this.includeNonString != includeNonString || (cb == null && root == null)) {
			createCriteriaQueryAndRoot();
			// entityColumnFields = buildColumnFields(entityClass, null);
			this.includeNonString = includeNonString;
			buildExampleFiled(example, "");
		}
		buildCriteria();
		criteriaQuery.select((Selection<? extends E>) cb.count(root));
		TypedQuery<E> retult = createQueryAndSetParameters();
		return retult;
	}

	public Query buildRandom(boolean includeNonString2) {
		if (this.includeNonString != includeNonString || (cb == null && root == null)) {
			createCriteriaQueryAndRoot();
			// entityColumnFields = buildColumnFields(entityClass, null);
			this.includeNonString = includeNonString;
			buildExampleFiled(example, "");
		}
		buildCriteria();
		javax.persistence.criteria.Order order = cb.asc(cb.function("random", Float.class, null));
		criteriaQuery.orderBy(order);
		criteriaQuery.select(root);
		TypedQuery<E> retult = createQueryAndSetParameters();
		return retult;
	}

	private void buildCriteria() {
		buildCriteriaForFieldsAndMatching();
		if (criteria.size() == 0) {
			log.warn("query by example running with no criteria");
		} else if (criteria.size() == 1) {
			criteriaQuery.where(criteria.get(0));
		} else {
			addJunctionCriteria(criteria);
		}
	}

	private void addOrderByToCriteria() {
		ArrayList<javax.persistence.criteria.Order> orders = new ArrayList<javax.persistence.criteria.Order>(
				ordering.size());
		javax.persistence.criteria.Order order;
		for (QBEOrder qbeOrder : ordering) {
			String tokens[] = qbeOrder.getOrderBy().split("\\.");
			System.out.println("QBEORDER:" + qbeOrder.getOrderBy());
			Path path = root;
			int i = 0;
			for (i = 0; i < tokens.length; i++) {
				path = path.get(tokens[i]);
			}
			switch (qbeOrder.getOrder()) {
			case ASCENDING:
				order = cb.asc(path);
				break;
			case DESCENDING:
				order = cb.desc(path);
				break;
			default:
				throw new UnsupportedOperationException("no handling implemented for orderType" + qbeOrder.getOrder());
			}
			orders.add(order);
		}
		if (orders.size() > 0) {
			criteriaQuery.orderBy(orders);
		}
	}

	private void buildCriteriaForFieldsAndMatching() {
		criteria = new ArrayList<Predicate>();

		for (String name : exampleFields.keySet()) {
			String nameId = entityColumnName.get(name);
			Field field = entityColumnFields.get(name);
			System.out.println("matching:" + ":" + name + ":" + field + ":" + nameId);
			if (nameId == null || field == null) {
				String format = "can not do %s matching on field %s of type %s";
				throw new UnsupportedOperationException(format(format, matching, name, field.getType()));
			}
			String[] tokens = nameId.split("\\.");
			int i = 0;

			Path path = root;
			for (i = 0; i < tokens.length - 1; i++) {
				path = path.get(tokens[i]);
			}
			path = path.get(tokens[i]);

			if (matching == Matching.EXACT) {
				criteria.add(cb.equal(path, (Expression<String>) cb.parameter(field.getType(), name)));
				continue;
			}
			if (caseQuery == Case.INSENSITIVE) {
				criteria.add(cb.like(cb.lower(path), cb.parameter(String.class, field.getName())));
			} else if (includeNonString || String.class == field.getType()) {
				criteria.add(cb.like(path, cb.parameter(String.class, field.getName())));
			}
		}

	}

	private void addJunctionCriteria(List<Predicate> criteria) {
		switch (junction) {
		case UNION:
			criteriaQuery.where(cb.and(criteria.toArray(new Predicate[criteria.size()])));
			break;
		case INTERSECTION:
			criteriaQuery.where(cb.or(criteria.toArray(new Predicate[criteria.size()])));
			break;
		default:
			String message = format("no case for %s %s in switch", Junction.class.getSimpleName(), junction);
			throw new UnsupportedOperationException(message);
		}
	}

	private void createCriteriaQueryAndRoot() {
		cb = entityManager.getCriteriaBuilder();
		criteriaQuery = cb.createQuery(entityClass);
		root = criteriaQuery.from(entityClass);
	}

	private TypedQuery<E> createQueryAndSetParameters() {
		String wildcardPrefix = "";
		String wildcardPostfix = "";

		TypedQuery<E> query = entityManager.createQuery(criteriaQuery);

		if (Matching.MIDDLE == matching || Matching.END == matching) {
			wildcardPrefix = "%";
		}
		if (Matching.MIDDLE == matching || Matching.START == matching) {
			wildcardPostfix = "%";
		}

		Object value;
		for (String name : exampleFields.keySet()) {
			Field field = entityColumnFields.get(name);
			Class<?> fieldType = field.getType();

			log.trace("Setting parameter for field [{}] with type [{}]", field, fieldType);

			if (String.class.equals(fieldType)) {
				log.trace("Field [{}] type is identified as a string", field);
				String valueString;

				valueString = wildcardPrefix + exampleFields.get(name) + wildcardPostfix;

				if (caseQuery == Case.INSENSITIVE) {
					value = valueString.toLowerCase();
				} else {
					value = valueString;
				}
			} else {
				log.trace("Field [{}] type is not identified as a string", field);
				value = exampleFields.get(field);
			}

			query.setParameter(name, value);
		}
		return query;
	}

	public static void main(String[] args) {
		int test = 0x4;
		SecuUserRole example = new SecuUserRole();
		SecuUserRolePK id = new SecuUserRolePK();
		example.setId(id);
		// id.setRoleId("AAA");
		id.setUserId("ZZ");
		// example.setMaintainUserId("A");
		EntityManager em = T49JPAPool.getEntitymanager();
		if ((test & 0x1) != 0) {
			QBEExample<SecuUserRole, SecuUserRole> util = QBE.using(em).query(SecuUserRole.class).by(example)
					.use(Matching.END);
			util.orderBy("maintainUserId", Order.ASCENDING);
			util.orderBy("id.userId", Order.ASCENDING);
			List<SecuUserRole> list = util.list();
			System.out.println("DONE....:" + list);
		}

		if ((test & 0x2) != 0) {
			KvSetting e = new KvSetting();
			e.setKeyId("0");
			QBEExample<KvSetting, KvSetting> util2 = QBE.using(em).query(KvSetting.class).by(e).use(Matching.EXACT);
			// QBEExample<KvSetting, KvSetting> util2 =
			// QBE.using(em).query(KvSetting.class).by(e).use(Matching.END);
			List<KvSetting> list2 = util2.list();
			System.out.println("DONE....:" + list2.size());
			// System.out.println("Select count=" + util2.getTotalCount());
		}
		if ((test & 0x04) != 0) {
			KvSetting e = new KvSetting();
			QBEExample<KvSetting, KvSetting> util2 = QBE.using(em).query(KvSetting.class).by(e).use(Matching.EXACT);
			for (int i = 0; i < 10; i++) {
				KvSetting kv = util2.randomItem();
				System.out.println("randomid:" + kv.getKeyId());
			}
			System.out.println("DONE....:");

		}
		if ((test & 0x04) != 0) {
			SecuUserRole e = new SecuUserRole();
			for (int i = 0; i < 10; i++) {
				QBEExample<SecuUserRole, SecuUserRole> util2 = QBE.using(em).query(SecuUserRole.class).by(e)
						.use(Matching.EXACT);
				SecuUserRole ur = util2.randomItem();
				System.out.println("randomid:" + ur.getId().getUserId() + ":" + ur.getId().getRoleId());
			}
			System.out.println("DONE....:");

		}
	}
}
