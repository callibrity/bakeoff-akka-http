package com.callibrity.bakeoff.domain;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import lombok.Value;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;


public class ArtistRepository extends AbstractBehavior<ArtistRepository.Command> {

// ------------------------------ FIELDS ------------------------------

    private final Publisher<? extends Connection> publisher;
    private final Scheduler scheduler;
// -------------------------- STATIC METHODS --------------------------

    public static Behavior<ArtistRepository.Command> create(Publisher<? extends Connection> publisher) {
        return Behaviors.setup(actorContext -> new ArtistRepository(actorContext, publisher));
    }

// --------------------------- CONSTRUCTORS ---------------------------

    public ArtistRepository(ActorContext<Command> context, Publisher<? extends Connection> publisher) {
        super(context);
        this.publisher = publisher;
        this.scheduler = Schedulers.single();

    }

// ------------------------ CANONICAL METHODS ------------------------

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(CreateArtist.class, this::onCreateArtist)
                .onMessage(DeleteArtist.class, this::onDeleteArtist)
                .onMessage(GetArtist.class, this::onGetArtist)
                .onMessage(UpdateArtist.class, this::onUpdateArtist)
                .build();
    }

// -------------------------- OTHER METHODS --------------------------

    private Behavior<Command> onCreateArtist(CreateArtist command) {
        final String id = UUID.randomUUID().toString();
        resultMono(conn -> conn.createStatement("insert into artist (id,name,genre) values ($1, $2, $3)")
                .bind("$1", id)
                .bind("$2", command.getName())
                .bind("$3", command.getGenre().name()))
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .map(rowsUpdated -> rowsUpdated > 0)
                .map(result -> new Artist(id, command.getName(), command.getGenre()))
                .subscribeOn(scheduler)
                .subscribe(command.getReplyTo()::tell);
        return this;
    }

    private Behavior<Command> onDeleteArtist(DeleteArtist command) {
        resultMono(conn -> conn.createStatement("delete from artist where id = $1").bind("$1", command.getArtistId()))
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .map(rowsUpdated -> rowsUpdated > 0)
                .defaultIfEmpty(false)
                .subscribeOn(scheduler)
                .subscribe(command.replyTo::tell);
        return this;
    }

    private Behavior<Command> onGetArtist(GetArtist command) {
        rowMono(conn -> conn.createStatement("select id, name, genre from artist where id = $1")
                .bind("$1", command.getArtistId()))
                .map(row -> Optional.of(new Artist(row.get(0, String.class), row.get(1, String.class), Enum.valueOf(Genre.class, row.get(2, String.class)))))
                .defaultIfEmpty(Optional.empty())
                .subscribeOn(scheduler)
                .subscribe(optional -> command.getReplyTo().tell(optional));
        return this;
    }

    private Behavior<Command> onUpdateArtist(UpdateArtist command) {
        resultMono(conn -> conn.createStatement("update artist set (name, genre) = ($1, $2) where id = $3")
                .bind("$1", command.getName())
                .bind("$2", command.getGenre().name())
                .bind("$3", command.getArtistId()))
                .flatMap(result -> Mono.from(result.getRowsUpdated()))
                .map(rowsUpdated -> rowsUpdated > 0)
                .defaultIfEmpty(false)
                .subscribeOn(scheduler)
                .subscribe(updated -> {
                    if (Boolean.TRUE.equals(updated)) {
                        command.replyTo.tell(Optional.of(new Artist(command.getArtistId(), command.getName(), command.getGenre())));
                    } else {
                        command.replyTo.tell(Optional.empty());
                    }
                });
        return this;
    }

    private Mono<Result> resultMono(Function<Connection, Statement> fn) {
        return Mono.usingWhen(publisher, conn -> Mono.from(fn.apply(conn).execute()), Connection::close);
    }

    private Mono<Row> rowMono(Function<Connection, Statement> fn) {
        return resultMono(fn)
                .flatMap(result -> Mono.from(result.map((row, meta) -> row)));
    }

// -------------------------- INNER CLASSES --------------------------

    public interface Command {

    }

    @Value
    public static class DeleteArtist implements Command {

// ------------------------------ FIELDS ------------------------------

        String artistId;
        ActorRef<Boolean> replyTo;

    }

    @Value
    public static class GetArtist implements Command {

// ------------------------------ FIELDS ------------------------------

        String artistId;
        ActorRef<Optional<Artist>> replyTo;

    }

    @Value
    public static class CreateArtist implements Command {

// ------------------------------ FIELDS ------------------------------

        String name;
        Genre genre;
        ActorRef<Artist> replyTo;

    }

    @Value
    public static class UpdateArtist implements Command {

// ------------------------------ FIELDS ------------------------------

        String artistId;
        String name;
        Genre genre;
        ActorRef<Optional<Artist>> replyTo;

    }

}
