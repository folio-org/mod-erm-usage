package org.folio.rest.impl3;

import org.folio.rest.Setup;
import org.folio.rest.SetupTenant;
import org.junit.jupiter.api.Nested;

/**
 * Entry point for all JUnit 5 integration tests in this package.
 *
 * <p>Failsafe excludes {@code org/folio/rest/impl3/*IT.java} (see {@code
 * mod-erm-usage-server/pom.xml}), so the IT classes in this package do not run on their own. Each
 * one runs as a {@link Nested} subclass of this suite instead.
 *
 * <p>{@link Setup} on the suite starts the expensive infrastructure once and shares it between all
 * nested test classes:
 *
 * <ul>
 *   <li>{@code VertxExtension}: Vert.x test support
 *   <li>{@code PostgresExtension}: a single Postgres Testcontainer
 *   <li>{@code RestVerticleExtension}: a single deployed {@code RestVerticle} on {@code
 *       TestUtils.getPort()}
 * </ul>
 *
 * <p>Tenants are set up per nested class, not per suite. Each IT declares {@link
 * SetupTenant @SetupTenant}. Its {@code TenantExtension} posts the tenant (optionally loading
 * sample data) before the class's tests run and purges it afterward. Nested classes run one after
 * another, so they can reuse the same tenant ID without seeing each other's data.
 *
 * <p>To add a new integration test:
 *
 * <ol>
 *   <li>Create a package-scoped {@code *IT} class in this package and annotate it with
 *       {@code @Setup} and, if it needs a tenant, {@code @SetupTenant}.
 *   <li>Register it here as {@code @Nested class FooITNested extends FooIT {}}.
 * </ol>
 *
 * <p>Run only this suite with {@code mvn verify -Dit.test=ITSuite}.
 */
@Setup
public class ITSuite {

  @Nested
  class CounterReportR5UploadITNested extends CounterReportR5UploadIT {}

  @Nested
  class CounterReportExportITNested extends CounterReportExportIT {}

  @Nested
  class UsageDataProvidersITNested extends UsageDataProvidersIT {}
}
