package io.highthroughput.demo.web;

import org.springframework.core.io.Resource;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.resource.PathResourceResolver;

import java.io.IOException;

/** Serves the built UI ({@code demo-ui/dist}); unknown non-API paths get {@code index.html}. */
public class UiConfiguration implements WebMvcConfigurer {

    private final String location;

    public UiConfiguration(String location) {
        this.location = location.endsWith("/") ? location : location + "/";
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        // The resource handler refuses an empty path, so the root needs its own mapping.
        registry.addViewController("/").setViewName("forward:/index.html");
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/**")
                .addResourceLocations(location)
                .resourceChain(false)
                .addResolver(new PathResourceResolver() {
                    @Override
                    protected Resource getResource(String path, Resource base) throws IOException {
                        Resource resource = super.getResource(path, base);
                        if (resource != null || path.startsWith("demo/") || path.contains(".")) {
                            return resource;
                        }
                        return super.getResource("index.html", base);
                    }
                });
    }
}
