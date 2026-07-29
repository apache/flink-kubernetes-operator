/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.api.docs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.source.doctree.DocCommentTree;
import com.sun.source.util.DocTrees;
import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;
import org.apache.commons.io.FileUtils;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.util.ElementScanner9;
import javax.lang.model.util.Types;

import java.io.File;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Doclet for generating the FlinkDeployment CRD reference. */
public class CrdReferenceDoclet implements Doclet {

    private static final String SPEC_PACKAGE_PREFIX =
            "org.apache.flink.kubernetes.operator.api.spec";
    private static final String STATUS_PACKAGE_PREFIX =
            "org.apache.flink.kubernetes.operator.api.status";

    /** Helper classes living in the CRD packages that are not part of the CRD data model. */
    private static final Set<String> EXCLUDED_CLASSES =
            Set.of(SPEC_PACKAGE_PREFIX + ".ConfigObjectNodeDeserializer");

    private DocTrees treeUtils;
    private String templateFile;
    private String outputFile;
    private Map<Element, Element> child2ParentElements;

    private String getNameOrJsonPropValue(Element e) {
        return e.getAnnotationMirrors().stream()
                .filter(
                        am ->
                                am.getAnnotationType()
                                        .toString()
                                        .equals(JsonProperty.class.getName()))
                .flatMap(am -> am.getElementValues().entrySet().stream())
                .filter(entry -> entry.getKey().getSimpleName().toString().equals("value"))
                .map(
                        entry -> {
                            AnnotationValue value = entry.getValue();
                            if (value.getValue() instanceof String) {
                                return (String) value.getValue();
                            }
                            return e.getSimpleName().toString();
                        })
                .findFirst()
                .orElse(e.getSimpleName().toString());
    }

    @Override
    public void init(Locale locale, Reporter reporter) {}

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public Set<? extends Option> getSupportedOptions() {
        return Set.of(
                new Option("--templateFile", true, "Template File", "filename") {
                    @Override
                    public boolean process(String option, List<String> arguments) {
                        templateFile = arguments.get(0);
                        return true;
                    }
                },
                new Option("--outputFile", true, "Output File", "filename") {
                    @Override
                    public boolean process(String option, List<String> arguments) {
                        outputFile = arguments.get(0);
                        return true;
                    }
                });
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    @Override
    public boolean run(DocletEnvironment environment) {
        treeUtils = environment.getDocTrees();
        try (PrintStream printStream = new PrintStream(outputFile)) {
            // Write template content first
            printStream.write(FileUtils.readFileToByteArray(new File(templateFile)));

            MdPrinter se = new MdPrinter(printStream);
            printStream.println();
            printStream.println("## Spec");
            var spec =
                    sortedByName(
                            environment.getIncludedElements().stream()
                                    .filter(e -> e.toString().startsWith(SPEC_PACKAGE_PREFIX))
                                    .filter(e -> !EXCLUDED_CLASSES.contains(e.toString()))
                                    .collect(Collectors.toSet()));
            handleAbstractClass(spec, environment.getTypeUtils());
            se.show(spec);

            printStream.println();
            printStream.println("## Status");
            var status =
                    sortedByName(
                            environment.getIncludedElements().stream()
                                    .filter(e -> e.toString().startsWith(STATUS_PACKAGE_PREFIX))
                                    .filter(e -> !EXCLUDED_CLASSES.contains(e.toString()))
                                    .collect(Collectors.toSet()));
            handleAbstractClass(status, environment.getTypeUtils());
            se.show(status);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleAbstractClass(Set<? extends Element> elements, Types typeUtils) {
        this.child2ParentElements = new HashMap<>();
        var classElements =
                elements.stream()
                        .filter(element -> element.getKind() == ElementKind.CLASS)
                        .collect(Collectors.toList());

        for (Element element : classElements) {
            if (element.getModifiers().contains(Modifier.ABSTRACT)) {
                var enclosedElements = element.getEnclosedElements();
                // do not print the abstract class's elements
                enclosedElements.forEach(elements::remove);
                elements.remove(element);
            } else {
                var directSuperTypes = typeUtils.directSupertypes(element.asType());
                if (directSuperTypes.size() == 1) {
                    var parentElement = typeUtils.asElement(directSuperTypes.get(0));
                    String name = parentElement.toString();
                    if (name.startsWith(SPEC_PACKAGE_PREFIX)
                            || name.startsWith(STATUS_PACKAGE_PREFIX)) {
                        child2ParentElements.put(element, parentElement);
                    }
                }
            }
        }
    }

    private String cleanDoc(String doc) {
        // Collapse all whitespace (tabs and newlines, e.g. between a description and its
        // @deprecated tag) into single spaces so block tags do not run into the preceding text.
        return doc.replaceAll("\\s+", " ").trim();
    }

    private boolean isPrintableField(Element e) {
        if (e.getKind() != ElementKind.FIELD || e.getModifiers().contains(Modifier.STATIC)) {
            return false;
        }
        var jsonIgnore = e.getAnnotation(JsonIgnore.class);
        return jsonIgnore == null || !jsonIgnore.value();
    }

    private boolean hasPrintableFields(Element classElement) {
        if (classElement.getEnclosedElements().stream().anyMatch(this::isPrintableField)) {
            return true;
        }
        var parent = child2ParentElements.get(classElement);
        return parent != null
                && parent.getEnclosedElements().stream().anyMatch(this::isPrintableField);
    }

    private Set<? extends Element> sortedByName(Set<? extends Element> elements) {
        Set<Element> out =
                new TreeSet<>((e1, e2) -> CharSequence.compare(e1.toString(), e2.toString()));
        out.addAll(elements);
        return out;
    }

    private class MdPrinter extends ElementScanner9<Void, Integer> {
        final PrintStream out;

        MdPrinter(PrintStream out) {
            this.out = out;
        }

        void show(Set<? extends Element> elements) {
            scan(elements, 0);
        }

        private void printFieldRows(Element classElement) {
            for (Element enclosed : classElement.getEnclosedElements()) {
                if (isPrintableField(enclosed)) {
                    DocCommentTree fieldDoc = treeUtils.getDocCommentTree(enclosed);
                    out.println(
                            "| "
                                    + getNameOrJsonPropValue(enclosed)
                                    + " | "
                                    + enclosed.asType().toString()
                                    + " | "
                                    + (fieldDoc != null ? cleanDoc(fieldDoc.toString()) : "")
                                    + " |");
                }
            }
        }

        @Override
        public Void scan(Element e, Integer depth) {
            DocCommentTree dcTree = treeUtils.getDocCommentTree(e);
            ElementKind kind = e.getKind();

            // Do not document ignored fields
            var jsonIgnore = e.getAnnotation(JsonIgnore.class);
            if (jsonIgnore != null && jsonIgnore.value()) {
                return null;
            }

            switch (kind) {
                case CLASS:
                    out.println();
                    out.println("### " + e.getSimpleName());
                    out.println("**Class**: " + e);
                    out.println();
                    out.println("**Description**: " + dcTree);
                    out.println();
                    // do not print an empty parameter table for field-less classes
                    if (hasPrintableFields(e)) {
                        out.println("| Parameter | Type | Docs |");
                        out.println("| ----------| ---- | ---- |");
                        // if this is a child class, print it's parent's fields first.
                        if (child2ParentElements.containsKey(e)) {
                            printFieldRows(child2ParentElements.get(e));
                        }
                        printFieldRows(e);
                    }
                    // nested types are documented through their own top-level entries
                    return null;
                case ENUM:
                    out.println();
                    out.println("### " + e.getSimpleName());
                    out.println("**Class**: " + e);
                    out.println();
                    out.println("**Description**: " + dcTree);
                    out.println();
                    out.println("| Value | Docs |");
                    out.println("| ----- | ---- |");
                    break;
                case ENUM_CONSTANT:
                    out.println(
                            "| "
                                    + getNameOrJsonPropValue(e)
                                    + " | "
                                    + (dcTree != null ? cleanDoc(dcTree.toString()) : "")
                                    + " |");
                    return null;
                default:
                    return null;
            }
            return super.scan(e, depth + 1);
        }
    }

    private abstract class Option implements Doclet.Option {
        private final String name;
        private final boolean hasArg;
        private final String description;
        private final String parameters;

        Option(String name, boolean hasArg, String description, String parameters) {
            this.name = name;
            this.hasArg = hasArg;
            this.description = description;
            this.parameters = parameters;
        }

        @Override
        public int getArgumentCount() {
            return hasArg ? 1 : 0;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public Kind getKind() {
            return Kind.STANDARD;
        }

        @Override
        public List<String> getNames() {
            return List.of(name);
        }

        @Override
        public String getParameters() {
            return hasArg ? parameters : "";
        }
    }
}
