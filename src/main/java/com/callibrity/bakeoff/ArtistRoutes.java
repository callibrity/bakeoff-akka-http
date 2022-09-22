package com.callibrity.bakeoff;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Scheduler;
import akka.actor.typed.javadsl.AskPattern;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.japi.function.Function;
import com.callibrity.bakeoff.domain.Artist;
import com.callibrity.bakeoff.domain.ArtistRepository;
import com.callibrity.bakeoff.domain.Genre;
import lombok.Data;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.Directives.*;

public class ArtistRoutes {

// ------------------------------ FIELDS ------------------------------

    private final ActorRef<ArtistRepository.Command> repositoryActor;
    private final Scheduler scheduler;
    private final Duration askTimeout;

// --------------------------- CONSTRUCTORS ---------------------------

    public ArtistRoutes(ActorSystem<?> system, ActorRef<ArtistRepository.Command> repositoryActor) {
        this.repositoryActor = repositoryActor;
        scheduler = system.scheduler();
        askTimeout = system.settings().config().getDuration("routes.ask-timeout");
    }

// -------------------------- OTHER METHODS --------------------------

    private Route createArtistRoute() {
        return entity(Jackson.unmarshaller(CreateArtistRequest.class), request -> onSuccess(createArtist(request), artist -> complete(StatusCodes.OK, artist, Jackson.marshaller())));
    }

    private CompletionStage<Artist> createArtist(CreateArtistRequest request) {
        return askRegistry(replyTo -> new ArtistRepository.CreateArtist(request.getName(), request.getGenre(), replyTo));
    }

    private <R> CompletionStage<R> askRegistry(Function<ActorRef<R>, ArtistRepository.Command> messageFactory) {
        return AskPattern.ask(repositoryActor, messageFactory, askTimeout, scheduler);
    }

    public Route routes() {
        return pathPrefix("api", () ->
                pathPrefix("artists", () ->
                        concat(pathEndOrSingleSlash(() ->
                                        concat(
                                                post(this::createArtistRoute)
                                        )
                                ),
                                path(StringUnmarshallers.STRING, artistId ->
                                        concat(
                                                get(() -> getArtistRoute(artistId)),
                                                put(() -> updateArtistRoute(artistId)),
                                                delete(() -> deleteArtistRoute(artistId))
                                        )
                                ))
                )
        );
    }

    private Route getArtistRoute(String artistId) {
        return onSuccess(getArtist(artistId), artistOptional ->
                artistOptional
                        .map(artist -> (Route) complete(StatusCodes.OK, artist, Jackson.marshaller()))
                        .orElseGet(() -> complete(StatusCodes.NOT_FOUND)));
    }

    private CompletionStage<Optional<Artist>> getArtist(String artistId) {
        return askRegistry(replyTo -> new ArtistRepository.GetArtist(artistId, replyTo));
    }

    private Route updateArtistRoute(String artistId) {
        return entity(Jackson.unmarshaller(UpdateArtistRequest.class), request -> onSuccess(updateArtist(artistId, request),
                artistOptional -> artistOptional
                        .map(artist -> (Route) complete(StatusCodes.OK, artist, Jackson.marshaller()))
                        .orElseGet(() -> complete(StatusCodes.NOT_FOUND))));
    }

    private CompletionStage<Optional<Artist>> updateArtist(String artistId, UpdateArtistRequest request) {
        return askRegistry(replyTo -> new ArtistRepository.UpdateArtist(artistId, request.getName(), request.getGenre(), replyTo));
    }

    private RouteAdapter deleteArtistRoute(String artistId) {
        return onSuccess(deleteArtist(artistId), added -> complete(StatusCodes.OK));
    }

    private CompletionStage<Boolean> deleteArtist(String artistId) {
        return askRegistry(replyTo -> new ArtistRepository.DeleteArtist(artistId, replyTo));
    }

// -------------------------- INNER CLASSES --------------------------

    @Data
    public static class CreateArtistRequest {

// ------------------------------ FIELDS ------------------------------

        private String name;
        private Genre genre;

    }

    @Data
    public static class UpdateArtistRequest {

// ------------------------------ FIELDS ------------------------------

        private String name;
        private Genre genre;

    }

}
