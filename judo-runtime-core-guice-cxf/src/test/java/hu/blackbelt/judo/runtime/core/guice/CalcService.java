package hu.blackbelt.judo.runtime.core.guice;

import com.google.inject.Inject;
import hu.blackbelt.judo.dao.api.DAO;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/calc")
public class CalcService {

    @Inject
    DAO dao;

    @GET
    @Path("/add/{a}/{b}")
    @Produces(MediaType.APPLICATION_JSON)
    public Double add(@PathParam("a") double a, @PathParam("b") double b) {
        return (a + b);
    }
}