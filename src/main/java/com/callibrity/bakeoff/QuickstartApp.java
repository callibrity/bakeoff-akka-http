package com.callibrity.bakeoff;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.PoolRouter;
import akka.actor.typed.javadsl.Routers;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.server.Route;
import com.callibrity.bakeoff.domain.ArtistRepository;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.flywaydb.core.Flyway;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

//#main-class
public class QuickstartApp {

// --------------------------- main() method ---------------------------

    public static void main(String[] args) throws Exception {
        final Config config = ConfigFactory.defaultApplication().resolve();

        Config dbConfig = config.getConfig("db");

        Flyway flyway = createFlyway(dbConfig);
        flyway.migrate();

        ConnectionFactory connectionFactory = createConnectionFactory(dbConfig);

        Behavior<NotUsed> rootBehavior = Behaviors.setup(context -> {
            int poolSize = 20;
            PoolRouter<ArtistRepository.Command> pool =
                    Routers.pool(
                            poolSize,
                            Behaviors.supervise(ArtistRepository.create(connectionFactory.create())).onFailure(SupervisorStrategy.restart()));
            ActorRef<ArtistRepository.Command> registryActor = context.spawn(pool, "worker-pool");

            ArtistRoutes artistRoutes = new ArtistRoutes(context.getSystem(), registryActor);
            startHttpServer(artistRoutes.routes(), context.getSystem());

            return Behaviors.empty();
        });
        ActorSystem.create(rootBehavior, "BakeoffAkkaHttpServer");
    }

    private static Flyway createFlyway(Config config) {
        final String url = String.format("jdbc:postgresql://%s:%d/%s", config.getString("host"), config.getInt("port"), config.getString("name"));
        Flyway flyway = Flyway.configure().dataSource(url, config.getString("user"), config.getString("pass")).load();
        return flyway;
    }

    private static ConnectionFactory createConnectionFactory(Config config) {
        return ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "pool")
                .option(PROTOCOL, "postgresql")
                .option(HOST, config.getString("host"))
                .option(PORT, config.getInt("port"))
                .option(USER, config.getString("user"))
                .option(PASSWORD, config.getString("pass"))
                .option(DATABASE, config.getString("name"))
                .build());
    }

    static void startHttpServer(Route route, ActorSystem<?> system) {
        CompletionStage<ServerBinding> futureBinding =
                Http.get(system).newServerAt("0.0.0.0", 8080).bind(route);

        futureBinding.whenComplete((binding, exception) -> {
            if (binding != null) {
                InetSocketAddress address = binding.localAddress();
                system.log().info("Server online at http://{}:{}/",
                        address.getHostString(),
                        address.getPort());
            } else {
                system.log().error("Failed to bind HTTP endpoint, terminating system", exception);
                system.terminate();
            }
        });
    }

}

//#main-class
