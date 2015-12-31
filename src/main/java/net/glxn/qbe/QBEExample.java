package net.glxn.qbe;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import net.glxn.qbe.exception.OrderCreationException;
import net.glxn.qbe.types.Case;
import net.glxn.qbe.types.Junction;
import net.glxn.qbe.types.Matching;
import net.glxn.qbe.types.Order;

/**
 * This class is where the QBE magic happens.
 * <p/>
 * Once you have an instance of this class you can call the following:<br/>
 * * {@link QBEExample#list()}<br/>
 * * {@link QBEExample#item()}<br/>
 * * {@link QBEExample#getQuery()}<br/>
 *
 * @param <T>
 *            the example class type
 * @param <E>
 *            the entity class type
 */
public class QBEExample<T, E> {

	private final QueryBuilder<T, E> queryBuilder;

	QBEExample(T example, Class<E> entityClass, EntityManager entityManager) {
		queryBuilder = new QueryBuilderT49<T, E>(example, entityClass, entityManager, Matching.EXACT, Junction.UNION,
				Case.SENSITIVE);
	}

	/**
	 * overrides the {@link Junction} on this object instance then returns
	 * itself
	 *
	 * @param junction
	 *            the {@link Junction} semantics you wish to use
	 * @return the QBEExample object instance
	 */
	public QBEExample<T, E> use(Junction junction) {
		queryBuilder.junction = junction;
		return this;
	}

	/**
	 * overrides the {@link Case} on this object instance then returns itself
	 *
	 * @param Case
	 *            {@link Case} you wish to use
	 * @return the QBEExample object instance
	 */
	public QBEExample<T, E> use(Case caseQuery) {
		queryBuilder.caseQuery = caseQuery;
		return this;
	}

	/**
	 * overrides the {@link Matching} on this object instance then returns
	 * itself
	 *
	 * @param matching
	 *            the {@link Matching} semantics you wish to use
	 * @return the QBEExample object instance
	 */
	public QBEExample<T, E> use(Matching matching) {
		queryBuilder.matching = matching;
		return this;
	}

	/**
	 * Defines the order of the result set
	 *
	 * @param orderBy
	 *            the property to order by, can be one of the following:
	 *            {@link javax.persistence.Column} annotated property on entity
	 *            or simply the field name on entity or example class
	 * @param order
	 *            signals the order to be ascending or descending
	 * @return the QBEExample object instance
	 * @throws net.glxn.qbe.exception.OrderCreationException
	 *             if the orderBy parameter can not be matched against any of
	 *             the following paths as described in the parameter doc
	 */
	public QBEExample<T, E> orderBy(String orderBy, Order order) throws OrderCreationException {
		queryBuilder.orderBy(orderBy, order);
		return this;
	}

	boolean includeNonString = false;

	public QBEExample<T, E> includeNonString() {
		includeNonString = true;
		return this;
	}

	/**
	 * get the {@link TypedQuery} that has been generating based on the example
	 * and entity.
	 *
	 * @return the typed query
	 */
	public TypedQuery<E> getQuery() {
		return queryBuilder.build(includeNonString);
	}

	/**
	 * gets results by calling
	 * {@link javax.persistence.TypedQuery#getResultList()}
	 * <em>NB: any exception thrown from the underlying TypedQuery call will bubble through</em>
	 *
	 * @return list of results
	 */
	public List<E> list() {
		return queryBuilder.build(includeNonString).getResultList();
	}

	/**
	 * gets item using {@link javax.persistence.TypedQuery#getSingleResult()}
	 * <br/>
	 * <br/>
	 * <em>NB: any exception thrown from the underlying TypedQuery call will bubble through</em>
	 *
	 * @return the single item matching the query
	 */
	public E item() {
		return queryBuilder.build(includeNonString).getSingleResult();
	}

	public TypedQuery getCountQuery() {
		return queryBuilder.buildCount(includeNonString);
	}

	public int getTotalCount() {
		return ((Long) queryBuilder.buildCount(includeNonString).getSingleResult()).intValue();
	}

	public E first() {
		List<E> list = queryBuilder.build(includeNonString).setFirstResult(0).setMaxResults(1).getResultList();
		if (list == null || list.isEmpty())
			return null;
		return list.get(0);
	}

	public E randomItem() {
		List<E> list = queryBuilder.buildRandom(includeNonString).setFirstResult(0).setMaxResults(1).getResultList();
		if (list == null || list.isEmpty())
			return null;
		return list.get(0);
	}


}
