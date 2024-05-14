import streamlit as st
from streamlit_option_menu import option_menu
import os
import pandas as pd
import json
import requests
import psycopg2
import plotly.express as py
from PIL import Image


#Data Frame Conversion
connection = psycopg2.connect(
    database="PhonePe",
    user="postgres",
    password="kk07ch30",
    host="localhost",
    port="5432"
)
Mycursor = connection.cursor()


#Aggregated_Insurance_DF
Mycursor.execute("Select * from aggregated_insurance")
connection.commit()
A = Mycursor.fetchall()

A_I_DF = pd.DataFrame(A, columns=("States","Years", "payment_mode", "Counts", "Amounts", "Payment_names", "Quarter"))


#Aggregated_Transaction_DF
Mycursor.execute("Select * from aggregated_transaction")
connection.commit()
B = Mycursor.fetchall()

A_T_DF = pd.DataFrame(B, columns=("States","Years", "payment_mode", "Counts", "Amounts", "Payment_names", "Quarter"))


#Aggregated_User_DF
Mycursor.execute("Select * from aggregated_user")
connection.commit()
C = Mycursor.fetchall()

A_U_DF = pd.DataFrame(C, columns=("Brands","Counts", "Percentages", "States", "Years", "Quarter"))


#Map_Insurance_DF
Mycursor.execute("Select * from map_insurance")
connection.commit()
D = Mycursor.fetchall()

M_I_DF = pd.DataFrame(D, columns=("District_Name","Amounts", "Counts", "Percentages", "States", "Years", "Quarter"))


#Map_Transaction_DF
Mycursor.execute("Select * from map_transaction")
connection.commit()
E = Mycursor.fetchall()

M_T_DF = pd.DataFrame(E, columns=("District_Name","Amounts", "Counts", "Percentages", "States", "Years", "Quarter"))


#Map_User_DF
Mycursor.execute("Select * from map_user")
connection.commit()
F = Mycursor.fetchall()

M_U_DF = pd.DataFrame(F, columns=("District_Name","AppOpens", "Registers", "Percentages", "States", "Years", "Quarter"))


#Top_Insurance_DF
Mycursor.execute("Select * from top_insurance")
connection.commit()
G = Mycursor.fetchall()

T_I_DF = pd.DataFrame(G, columns=("EntityName","Counts", "Amounts", "Percentages", "States", "Years", "Quarter"))


#Top_Transaction_DF
Mycursor.execute("Select * from top_transaction")
connection.commit()
H = Mycursor.fetchall()

T_T_DF = pd.DataFrame(H, columns=("EntityName","Counts", "Amounts", "Percentages", "States", "Years", "Quarter"))


#Top_User_DF
Mycursor.execute("Select * from top_user")
connection.commit()
I = Mycursor.fetchall()

T_U_DF = pd.DataFrame(I, columns=("EntityName","RegisteredUsers", "Percentages", "States", "Years", "Quarter"))


# def DataFrame_S_C_A_by_year(DF, Year):
#     output_Y = DF[DF["Years"]==Year].reset_index(drop= True)
#     output_Y =output_Y.groupby("States")[["Counts", "Amounts"]].sum().reset_index()

#     presentation_amount = py.bar(output_Y, x="States", y="Amounts", color_discrete_sequence=['purple'] ,title=f"{Year} - Bar Chart of Amounts by State")
#     presentation_amount.show()

#     presentation_count = py.bar(output_Y, x="States", y="Counts", color_discrete_sequence=['green'] ,title= f"{Year} - Bar Chart of Counts by State")
#     presentation_count.show()


def DataFrame_S_C_A_by_year(DF, Year):
        output_Y = DF[DF["Years"]==Year]
        output_Y.reset_index(drop = True, inplace = True)

        output_Y_GroupBy = output_Y.groupby("States")[["Counts", "Amounts"]].sum()
        output_Y_GroupBy.reset_index(inplace = True)

        presentation_amount = py.bar(output_Y_GroupBy, x="States", y="Amounts", color_discrete_sequence=['purple'], title=f"{Year} Year's - Bar Chart of Amounts by State")
        presentation_count = py.bar(output_Y_GroupBy, x="States", y="Counts", color_discrete_sequence=['green'], title=f"{Year} Year's - Bar Chart of Counts by State")
        
        st.plotly_chart(presentation_amount)
        st.plotly_chart(presentation_count)


        
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        j_response = json.loads(response.content)

        state_list = []
        for info in j_response["features"]:
            state_list.append(info['properties']['ST_NM'])

        state_list.sort()

        presentation_India = py.choropleth(output_Y_GroupBy, geojson= j_response, locations= "States", 
                                           featureidkey= "properties.ST_NM", color= "Amounts", color_continuous_scale= "Rainbow", 
                                           range_color= (output_Y_GroupBy["Amounts"].min(), output_Y_GroupBy["Amounts"].max()),
                                           hover_name= "States", title= f"{Year} Year's - Map of Transaction Amount", fitbounds= "locations")
        
        presentation_India.update_geos(visible = False)
        st.plotly_chart(presentation_India)

        presentation_India2 = py.choropleth(output_Y_GroupBy, geojson= j_response, locations= "States", 
                                           featureidkey= "properties.ST_NM", color= "Counts", color_continuous_scale= "Rainbow", 
                                           range_color= (output_Y_GroupBy["Counts"].min(), output_Y_GroupBy["Counts"].max()),
                                           hover_name= "States", title= f"{Year} Year's - Map of Transaction Count", fitbounds= "locations")
        
        presentation_India2.update_geos(visible = False)
        st.plotly_chart(presentation_India2)

        return output_Y


def DataFrame_S_C_A_by_Quarter(DF, Quarters):
        output_Y = DF[DF["Quarter"]==Quarters]
        output_Y.reset_index(drop = True, inplace = True)

        output_Y_GroupBy = output_Y.groupby("States")[["Counts", "Amounts"]].sum()
        output_Y_GroupBy.reset_index(inplace = True)

        presentation_amount = py.bar(output_Y_GroupBy, x="States", y="Amounts",
                                      color_discrete_sequence=['purple'],
                                        title=f"{output_Y["Years"].min()} YEAR, {Quarters} Quarter's - Bar Chart of Amounts by State")
        st.plotly_chart(presentation_amount)

        presentation_count = py.bar(output_Y_GroupBy, x="States", y="Counts",
                                     color_discrete_sequence=['green'],
                                       title=f"{output_Y["Years"].min()} YEAR, {Quarters} Quarter's - Bar Chart of Counts by State")
        st.plotly_chart(presentation_count)
   


        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        j_response = json.loads(response.content)

        state_list = []
        for info in j_response["features"]:
            state_list.append(info['properties']['ST_NM'])

        state_list.sort()

        presentation_India = py.choropleth(output_Y_GroupBy, geojson= j_response, locations= "States", 
                                           featureidkey= "properties.ST_NM", color= "Amounts", color_continuous_scale= "Rainbow", 
                                           range_color= (output_Y_GroupBy["Amounts"].min(), output_Y_GroupBy["Amounts"].max()),
                                           hover_name= "States", title= f"{output_Y["Years"].min()} YEAR, {Quarters} Quarter's - Map of Transaction Amount", fitbounds= "locations")
        
        presentation_India.update_geos(visible = False)
        st.plotly_chart(presentation_India)

        presentation_India2 = py.choropleth(output_Y_GroupBy, geojson= j_response, locations= "States", 
                                           featureidkey= "properties.ST_NM", color= "Counts", color_continuous_scale= "Rainbow", 
                                           range_color= (output_Y_GroupBy["Counts"].min(), output_Y_GroupBy["Counts"].max()),
                                           hover_name= "States", title= f"{output_Y["Years"].min()} YEAR, {Quarters} Quarter's - Map of Transaction Count", fitbounds= "locations")
        
        presentation_India2.update_geos(visible = False)
        st.plotly_chart(presentation_India2)

        return output_Y


def DataFrame_S_C_A_by_year2(DF, state):
    output_Y2 = DF[DF["States"] == state]
    output_Y2.reset_index(drop=True, inplace=True)

    output_Y2_GroupBy = output_Y2.groupby("Payment_names")[["Counts", "Amounts"]].sum()
    output_Y2_GroupBy.reset_index(inplace= True)


    fig_pie1 = py.pie(data_frame= output_Y2_GroupBy, names= "Payment_names", values= "Amounts",
                    title= f"{state} - Pie Chart of Amounts From Aggregated Transaction", hole= 0.5)
    st.plotly_chart(fig_pie1)

    fig_pie2 = py.pie(data_frame= output_Y2_GroupBy, names= "Payment_names", values= "Counts",
                    title= f"{state} - Pie Chart of Counts From Aggregated Transaction", hole= 0.5)
    st.plotly_chart(fig_pie2)



# def DataFrame_S_C_A_by_year3(DF, Year): 
          
#     output_Y3 = DF[DF["Years"] == Year]
#     output_Y3.reset_index(drop= True, inplace= True)

#     output_Y3_GroupBy = pd.DataFrame(output_Y3.groupby("Brands")[["Counts", "Percentages"]].sum())
#     output_Y3_GroupBy.reset_index(inplace= True)

#     presentation_3_DF = py.bar(output_Y3_GroupBy, x="Brands", y="Counts",
#                             color_discrete_sequence=['blue'],
#                             title=f"{Year} - Bar chart Of Brands And Counts for Aggregated User",
#                             hover_name= "Brands")

#     st.plotly_chart(presentation_3_DF)
    # return output_Y3    


def DataFrame_S_C_A_by_year3(DF, Year):
    output_Y3 = DF[DF["Years"] == Year]
    output_Y3.reset_index(drop= True, inplace= True)

    output_Y3_GroupBy = pd.DataFrame(output_Y3.groupby(["Brands", "States"])[["Counts", "Percentages"]].sum())
    output_Y3_GroupBy.reset_index(inplace= True)

    presentation_3_DF = py.bar(output_Y3_GroupBy, x="Brands", y="Counts",
                            color_discrete_sequence=['blue'],
                            title=f"{Year} - Bar chart Of Brands And Counts for Aggregated User",
                            hover_name= "States")

    st.plotly_chart(presentation_3_DF)
    return output_Y3



def DataFrame_S_C_A_by_Quarter3(DF, Quarters):
    output_Y3_Q = DF[DF["Quarter"] == Quarters]
    output_Y3_Q.reset_index(drop= True, inplace= True)

    output_Y3_Q_GroupBy = pd.DataFrame(output_Y3_Q.groupby(["Brands", "States"])[["Counts"]].sum())
    output_Y3_Q_GroupBy.reset_index(inplace= True)


    presentation_3_DF_AU = py.bar(output_Y3_Q_GroupBy,
                                x="Brands", y="Counts",
                                color_discrete_sequence=['blue'],
                                hover_name = "States",
                                title=f"{output_Y3_Q["Years"].min()} YEAR, {Quarters} Quarter's - Bar chart Of Brands And Counts")

    st.plotly_chart(presentation_3_DF_AU)
    return output_Y3_Q



def DataFrame_S_C_A_by_year3_State(DF, state):
    output_Y3_Q_S =  DF[DF["States"] == state]
    output_Y3_Q_S.reset_index(drop = True, inplace = True)

    presentation_3_S = py.line(output_Y3_Q_S, x = "Brands", y = "Counts", hover_data= "Percentages",
                            title = f"Line Chart for Brands in {state.upper()}",
                            markers = True)

    st.plotly_chart(presentation_3_S)



def DataFrame_D_C_A_by_year4(DF, state):
    output_Y4 = DF[DF["States"] == state]
    output_Y4.reset_index(drop=True, inplace=True)

    output_Y4_GroupBy = output_Y4.groupby("District_Name")[["Amounts", "Counts"]].sum()
    output_Y4_GroupBy.reset_index(inplace= True)


    fig_bar4 = py.bar(output_Y4_GroupBy,
                        x="Amounts", y="District_Name",
                        color_discrete_sequence=['blue'], orientation= "h",
                        hover_name = "District_Name",
                        title=f"{output_Y4["Years"].min()} YEAR, {state} States - Bar chart Of Amounts")
    
    fig_bar4.update_yaxes(tickfont=dict(size=11))
    st.plotly_chart(fig_bar4)

    fig_bar4 = py.bar(output_Y4_GroupBy,
                        x="Counts", y="District_Name",
                        color_discrete_sequence=['blue'],
                        hover_name = "District_Name",
                        title=f"{output_Y4["Years"].min()} YEAR, {state} States - Bar chart Of Counts")
    
    fig_bar4.update_yaxes(tickfont=dict(size=11))
    st.plotly_chart(fig_bar4)




def DataFrame_S_C_A_by_year6(DF, Year):
    output_Y6 = DF[DF["Years"] == Year]
    output_Y6.reset_index(drop= True, inplace= True)

    output_Y6_GroupBy = pd.DataFrame(output_Y6.groupby(["States", "District_Name"])[["Registers", "AppOpens"]].sum())
    output_Y6_GroupBy.reset_index(inplace= True)

    presentation_Y6 = py.line(output_Y6_GroupBy, x = "States", y = ["Registers", "AppOpens"], hover_data= "District_Name",
                                color_discrete_sequence=['blue', 'green'], title = f"{output_Y6["Years"].min()} YEAR - Line Chart for Registers & AppOpens",
                                markers = True)

    st.plotly_chart(presentation_Y6)

    return output_Y6    




def DataFrame_S_C_A_by_Q6(DF, quarter):
    output_Q6 = DF[DF["Quarter"] == quarter]
    output_Q6.reset_index(drop= True, inplace= True)

    output_Q6_GroupBy = pd.DataFrame(output_Q6.groupby(["States", "District_Name"])[["Registers", "AppOpens"]].sum())
    output_Q6_GroupBy.reset_index(inplace= True)
    # output_Q6_GroupBy

    presentation_Q6 = py.line(output_Q6_GroupBy, x = "States", y = ["Registers", "AppOpens"], hover_data= "District_Name",
                                color_discrete_sequence=['blue', 'green'], title = f"{quarter} QUARTER - Line Chart for Registers & AppOpens",
                                markers = True)

    st.plotly_chart(presentation_Q6)

    return output_Q6




def DataFrame_D_C_A_by_state6(DF, state):
    output_S6 = DF[DF["States"] == state]
    output_S6.reset_index(drop=True, inplace=True)


        # output_Y4_GroupBy = output_Y4.groupby("District_Name")[["Amounts", "Counts"]].sum()
        # output_Y4_GroupBy.reset_index(inplace= True)


    Presentation_Reg_S6 = py.bar(output_S6,
                        x="Registers", y="District_Name",
                        color_discrete_sequence=['blue'], orientation= "h",
                        hover_name = "States",
                        title=f"{output_S6["Years"].min()} YEAR, {output_S6["States"].min()}, {output_S6["Quarter"].min()} QUARTER - Bar chart Of Register Users")

    Presentation_Reg_S6.update_yaxes(tickfont=dict(size=11))
    st.plotly_chart(Presentation_Reg_S6)


    Presentation_App_S6 = py.bar(output_S6,
                        x= "AppOpens", y="District_Name",
                        color_discrete_sequence=['blue'], orientation= "h",
                        hover_name = "States",
                        title="Bar chart Of AppOppens")

    Presentation_App_S6.update_yaxes(tickfont=dict(size=11))
    st.plotly_chart(Presentation_App_S6)





def DataFrame_S_C_A_by_State7(DF, states):
    output_Y7_Q = DF[DF["States"] == states]
    output_Y7_Q.reset_index(drop= True, inplace= True)

    presentation7S = py.bar(output_Y7_Q,
                                x="Quarter", y="Amounts",
                                color_discrete_sequence=['blue'], height= 800, hover_data= "EntityName",
                                title=f"{output_Y7_Q["Years"].min()} YEAR, {output_Y7_Q["States"].min()} STATES - Bar chart Of Amounts")

    st.plotly_chart(presentation7S)
    # presentation7S.show()
    # return output_Y7_Q    



# def DataFrame_S_C_A_by_State7(DF, states):
#     output_Y7_Q = DF[DF["States"] == states ]
#     output_Y7_Q.reset_index(drop=True, inplace=True)

#     presentation7S = py.bar(output_Y7_Q,
#                              x="Quarter", y="Amounts",
#                              color="EntityName",
#                              title=f"{output_Y7_Q['Years'].min()} YEAR, {output_Y7_Q['States'].min()} STATES - Bar chart Of Amounts",
#                              height=800)

#     st.plotly_chart(presentation7S)
#     return output_Y7_Q



def DataFrame_S_C_A_by_Year9(DF, year):
    output_Y9_Q = DF[DF["Years"] == year]
    output_Y9_Q.reset_index(drop=True, inplace=True)

    output_Y9_Q_GroupBy = pd.DataFrame(output_Y9_Q.groupby(["States", "Quarter"])["RegisteredUsers"].sum())
    output_Y9_Q_GroupBy.reset_index(inplace=True)

    presentation9 = py.bar(output_Y9_Q_GroupBy, x="States", y="RegisteredUsers", color="Quarter",
                            title=f"{output_Y9_Q['Years'].min()} YEAR - Bar chart Of Registered Users",
                            labels={"RegisteredUsers": "Registered Users"},
                            color_discrete_map={1: "blue", 2: "green", 3: "red", 4: "orange"},
                            hover_name= "States")

    # presentation9.show()

    st.plotly_chart(presentation9)
    return output_Y9_Q



def DataFrame_S_C_A_by_S9(DF, state):
    output_S9 = DF[DF["States"] == state]
    output_S9.reset_index(drop= True, inplace= True)


    presentation_S9 = py.bar(output_S9, x = "Quarter", y = "RegisteredUsers",
                            title= "Bar chart for Registered Users", color="RegisteredUsers",
                            color_discrete_map={1: "blue", 2: "orange", 3: "red", 4: "green"},
                            hover_data= "EntityName")

    # presentation_S9.show()
    st.plotly_chart(presentation_S9)
    # return output_S9



#Chart - Questions and Answers
def Get_ALLChart_Counts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query1 = f'''
    select states,
    sum(counts) as "Transacation Count"
    from {sql}
    group by states
    order by "Transacation Count" desc
    limit 5'''

    Mycursor.execute(query1)
    sql1 = Mycursor.fetchall()
    connection.commit()

    DFQ1 = pd.DataFrame(sql1, columns= ("States", "Transacation Count"))
    DFQ1.index = range(1,len(DFQ1) + 1)

    presentation_amount_Sql1 = py.bar(DFQ1, x="States", y="Transacation Count", color_discrete_sequence=['purple'], title=f"Top 5 Transaction Counts")
    st.plotly_chart(presentation_amount_Sql1)


    query2 = f'''
    select states,
    sum(counts) as "Transacation Count"
    from {sql}
    group by states
    order by "Transacation Count"
    limit 5'''

    Mycursor.execute(query2)
    Sql2 = Mycursor.fetchall()
    connection.commit()

    DFQ2 = pd.DataFrame(Sql2, columns= ("States", "Transacation Count"))
    DFQ2.index = range(1,len(DFQ2) + 1)

    presentation_amount_Sql2 = py.bar(DFQ2, x="States", y="Transacation Count", color_discrete_sequence=['purple'], title=f"Bottom 5 Transaction Counts")
    st.plotly_chart(presentation_amount_Sql2)


    query3 = f'''
    select states,
    avg(counts) as "Transacation Count"
    from {sql}
    group by states
    order by "Transacation Count"'''

    Mycursor.execute(query3)
    Sql3 = Mycursor.fetchall()
    connection.commit()

    DFQ3 = pd.DataFrame(Sql3, columns= ("States", "Transacation Count"))
    DFQ3.index = range(1,len(DFQ3) + 1)

    presentation_amount_Sql3 = py.bar(DFQ3, x="States", y="Transacation Count", color_discrete_sequence=['purple'], title=f"Average Transaction Counts")
    st.plotly_chart(presentation_amount_Sql3)


def Get_ALLChart_Amounts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query1 = f'''
    select states,
    sum(amounts) as "Transacation Amounts"
    from {sql}
    group by states
    order by "Transacation Amounts" desc
    limit 5'''

    Mycursor.execute(query1)
    sql1 = Mycursor.fetchall()
    connection.commit()

    DFQ1 = pd.DataFrame(sql1, columns= ("States", "Transacation Amount"))
    DFQ1.index = range(1,len(DFQ1) + 1)

    presentation_amount_Sql1 = py.bar(DFQ1, x="States", y="Transacation Amount", color_discrete_sequence=['purple'], title=f"Top 5 Transaction Amounts")
    st.plotly_chart(presentation_amount_Sql1)


    query2 = f'''
    select states,
    sum(amounts) as "Transacation Amounts"
    from {sql}
    group by states
    order by "Transacation Amounts"
    limit 5'''

    Mycursor.execute(query2)
    Sql2 = Mycursor.fetchall()
    connection.commit()

    DFQ2 = pd.DataFrame(Sql2, columns= ("States", "Transacation Amount"))
    DFQ2.index = range(1,len(DFQ2) + 1)

    presentation_amount_Sql2 = py.bar(DFQ2, x="States", y="Transacation Amount", color_discrete_sequence=['purple'], title=f"Bottom 5 Transaction Amounts")
    st.plotly_chart(presentation_amount_Sql2)


    query3 = f'''
    select states,
    avg(amounts) as "Transacation Amounts"
    from {sql}
    group by states
    order by "Transacation Amounts"'''

    Mycursor.execute(query3)
    Sql3 = Mycursor.fetchall()
    connection.commit()

    DFQ3 = pd.DataFrame(Sql3, columns= ("States", "Transacation Amount"))
    DFQ3.index = range(1,len(DFQ3) + 1)

    presentation_amount_Sql3 = py.bar(DFQ3, x="States", y="Transacation Amount", color_discrete_sequence=['purple'], title=f"Average Transaction Amounts")
    st.plotly_chart(presentation_amount_Sql3)    


#Chart - Questions and Answers
def Get_TopChart_Amounts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query1 = f'''
    select states,
    sum(amounts) as "Transacation Amount"
    from {sql}
    group by states
    order by "Transacation Amount" desc
    limit 5'''

    Mycursor.execute(query1)
    sql1 = Mycursor.fetchall()
    connection.commit()

    DFQ1 = pd.DataFrame(sql1, columns= ("States", "Transacation Amount"))
    DFQ1.index = range(1,len(DFQ1) + 1)

    presentation_amount_Sql1 = py.bar(DFQ1, x="States", y="Transacation Amount", color_discrete_sequence=['purple'], title="Top 5 Transaction Amounts")
    st.plotly_chart(presentation_amount_Sql1)


def Get_TopChart_Counts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query1 = f'''
    select states,
    sum(counts) as "Transacation Counts"
    from {sql}
    group by states
    order by "Transacation Counts" desc
    limit 5'''

    Mycursor.execute(query1)
    sql1 = Mycursor.fetchall()
    connection.commit()

    DFQ1 = pd.DataFrame(sql1, columns= ("States", "Transacation Counts"))
    DFQ1.index = range(1,len(DFQ1) + 1)

    presentation_amount_Sql1 = py.bar(DFQ1, x="States", y="Transacation Counts", color_discrete_sequence=['purple'], title="Top 5 Transaction Counts")
    st.plotly_chart(presentation_amount_Sql1)


def Get_BottomChart_Amounts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query2 = f'''
    select states,
    sum(amounts) as "Transacation Amount"
    from {sql}
    group by states
    order by "Transacation Amount"
    limit 5'''

    Mycursor.execute(query2)
    sql2 = Mycursor.fetchall()
    connection.commit()

    DFQ2 = pd.DataFrame(sql2, columns= ("States", "Transacation Amount"))
    DFQ2.index = range(1,len(DFQ2) + 1)

    presentation_amount_Sql2 = py.bar(DFQ2, x="States", y="Transacation Amount", color_discrete_sequence=['purple'], title="Bottom 5 Transaction Amounts")
    st.plotly_chart(presentation_amount_Sql2)


def Get_BottomChart_Counts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query2 = f'''
    select states,
    sum(counts) as "Transacation Counts"
    from {sql}
    group by states
    order by "Transacation Counts"
    limit 5'''

    Mycursor.execute(query2)
    sql2 = Mycursor.fetchall()
    connection.commit()

    DFQ2 = pd.DataFrame(sql2, columns= ("States", "Transacation Counts"))
    DFQ2.index = range(1,len(DFQ2) + 1)

    presentation_amount_Sql2 = py.bar(DFQ2, x="States", y="Transacation Counts", color_discrete_sequence=['purple'], title="Bottom 5 Transaction Counts")
    st.plotly_chart(presentation_amount_Sql2)    



#Chart - Questions and Answers
def Get_AVGChart_Amounts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query3 = f'''
    select states,
    avg(amounts) as "Transacation Amount"
    from {sql}
    group by states
    order by "Transacation Amount"'''

    Mycursor.execute(query3)
    Sql3 = Mycursor.fetchall()
    connection.commit()

    DFQ3 = pd.DataFrame(Sql3, columns= ("States", "Transacation Amount"))
    DFQ3.index = range(1,len(DFQ3) + 1)

    presentation_amount_Sql3 = py.bar(DFQ3, x="States", y="Transacation Amount", color_discrete_sequence=['purple'], title=f"Average Transaction Amounts")
    st.plotly_chart(presentation_amount_Sql3)


def Get_AVGChart_Counts(sql):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query3 = f'''
    select states,
    avg(counts) as "Transacation Counts"
    from {sql}
    group by states
    order by "Transacation Counts"'''

    Mycursor.execute(query3)
    Sql3 = Mycursor.fetchall()
    connection.commit()

    DFQ3 = pd.DataFrame(Sql3, columns= ("States", "Transacation Counts"))
    DFQ3.index = range(1,len(DFQ3) + 1)

    presentation_amount_Sql3 = py.bar(DFQ3, x="States", y="Transacation Counts", color_discrete_sequence=['purple'], title=f"Average Transaction Counts")
    st.plotly_chart(presentation_amount_Sql3)    



#Chart - Questions and Answers
def Get_ALLChart_Registers(sql, state):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query1 = f'''
    select states,district_name,
    sum(registers) as total_registered_users
    from {sql}
    where states = '{state}'
    group by states, district_name
    order by total_registered_users desc
    limit 10;'''

    Mycursor.execute(query1)
    sql1 = Mycursor.fetchall()
    connection.commit()

    DFQ1 = pd.DataFrame(sql1, columns=("States", "District", "RegisteredUsers"))
    DFQ1.index = range(1, len(DFQ1) + 1)

    presentation_amount_Sql1 = py.bar(DFQ1, x="District", y="RegisteredUsers",
                                      hover_name="States", color_discrete_sequence=['purple'],
                                      title=f"Top 10 RegisteredUsers from {state}")
    st.plotly_chart(presentation_amount_Sql1)

    query2 = f'''
    select states,district_name,
    sum(registers) as total_registered_users
    from {sql}
    where states = '{state}'
    group by states, district_name
    order by total_registered_users
    limit 10;'''

    Mycursor.execute(query2)
    sql2 = Mycursor.fetchall()
    connection.commit()

    DFQ2 = pd.DataFrame(sql2, columns=("States", "District", "RegisteredUsers"))
    DFQ2.index = range(1, len(DFQ2) + 1)

    presentation_amount_Sql2 = py.bar(DFQ2, x="District", y="RegisteredUsers",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title=f"Bottom 10 RegisteredUsers from {state}")
    st.plotly_chart(presentation_amount_Sql2)

    query3 = f'''
    select states,district_name,
    avg(registers) as avg_registered_users
    from {sql}
    where states = '{state}'
    group by states, district_name;'''

    Mycursor.execute(query3)
    sql3 = Mycursor.fetchall()
    connection.commit()

    DFQ3 = pd.DataFrame(sql3, columns=("States", "District", "RegisteredUsers"))
    DFQ3.index = range(1, len(DFQ3) + 1)

    presentation_amount_Sql3 = py.bar(DFQ3, x="District", y="RegisteredUsers",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       width=1000, height=600, title=f"Average RegisteredUsers from {state}")
    st.plotly_chart(presentation_amount_Sql3)

    query4 = f'''
    select states,district_name,
    sum(registers) as total_registered_users
    from {sql}
    group by states, district_name
    order by total_registered_users desc
    limit 10;'''

    Mycursor.execute(query4)
    sql4 = Mycursor.fetchall()
    connection.commit()

    DFQ4 = pd.DataFrame(sql4, columns=("States", "District", "RegisteredUsers"))
    DFQ4.index = range(1, len(DFQ4) + 1)

    presentation_amount_Sql4 = py.bar(DFQ4, x="District", y="RegisteredUsers",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title="Top 10 RegisteredUsers from all States")
    st.plotly_chart(presentation_amount_Sql4)

    query5 = f'''
    select states,district_name,
    sum(registers) as total_registered_users
    from {sql}
    group by states, district_name
    order by total_registered_users
    limit 10;'''

    Mycursor.execute(query5)
    sql5 = Mycursor.fetchall()
    connection.commit()

    DFQ5 = pd.DataFrame(sql5, columns=("States", "District", "RegisteredUsers"))
    DFQ5.index = range(1, len(DFQ5) + 1)

    presentation_amount_Sql5 = py.bar(DFQ5, x="District", y="RegisteredUsers",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title="Bottom 10 RegisteredUsers from all States")
    st.plotly_chart(presentation_amount_Sql5)

    query6 = f'''
    select states,district_name,
    avg(registers) as avg_registered_users
    from {sql}
    group by states, district_name;'''

    Mycursor.execute(query6)
    sql6 = Mycursor.fetchall()
    connection.commit()

    DFQ6 = pd.DataFrame(sql6, columns=("States", "District", "RegisteredUsers"))
    DFQ6.index = range(1, len(DFQ6) + 1)

    presentation_amount_Sql6 = py.bar(DFQ6, x="District", y="RegisteredUsers",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title="Average RegisteredUsers from all States")
    st.plotly_chart(presentation_amount_Sql6)

    Mycursor.close()
    connection.close()   



#Chart - Questions and Answers
def Get_ALLChart_Appopens(sql,state):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )

    Mycursor = connection.cursor()

    query1 = f'''
    select states,district_name,
    sum(appopens) as total_appopens
    from {sql}
    where states = '{state}'
    group by states, district_name
    order by total_appopens desc
    limit 10;'''

    Mycursor.execute(query1)
    sql1 = Mycursor.fetchall()
    connection.commit()

    DFQ1 = pd.DataFrame(sql1, columns=("States", "District", "AppOpens"))
    DFQ1.index = range(1, len(DFQ1) + 1)

    presentation_amount_Sql1 = py.bar(DFQ1, x="District", y="AppOpens",
                                      hover_name="States", color_discrete_sequence=['purple'],
                                      title=f"Top 10 AppOpens from {state}")
    st.plotly_chart(presentation_amount_Sql1)

    query2 = f'''
    select states,district_name,
    sum(appopens) as total_appopens
    from {sql}
    where states = '{state}'
    group by states, district_name
    order by total_appopens
    limit 10;'''

    Mycursor.execute(query2)
    sql2 = Mycursor.fetchall()
    connection.commit()

    DFQ2 = pd.DataFrame(sql2, columns=("States", "District", "AppOpens"))
    DFQ2.index = range(1, len(DFQ2) + 1)

    presentation_amount_Sql2 = py.bar(DFQ2, x="District", y="AppOpens",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title=f"Bottom 10 AppOpens from {state}")
    st.plotly_chart(presentation_amount_Sql2)

    query3 = f'''
    select states,district_name,
    avg(appopens) as avg_appopens
    from {sql}
    where states = '{state}'
    group by states, district_name;'''

    Mycursor.execute(query3)
    sql3 = Mycursor.fetchall()
    connection.commit()

    DFQ3 = pd.DataFrame(sql3, columns=("States", "District", "AppOpens"))
    DFQ3.index = range(1, len(DFQ3) + 1)

    presentation_amount_Sql3 = py.bar(DFQ3, x="District", y="AppOpens",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       width=1000, height=600, title=f"Average AppOpens from {state}")
    st.plotly_chart(presentation_amount_Sql3)

    query4 = f'''
    select states,district_name,
    sum(appopens) as total_appopens
    from {sql}
    group by states, district_name
    order by total_appopens desc
    limit 10;'''

    Mycursor.execute(query4)
    sql4 = Mycursor.fetchall()
    connection.commit()

    DFQ4 = pd.DataFrame(sql4, columns=("States", "District", "AppOpens"))
    DFQ4.index = range(1, len(DFQ4) + 1)

    presentation_amount_Sql4 = py.bar(DFQ4, x="District", y="AppOpens",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title="Top 10 AppOpens from all States")
    st.plotly_chart(presentation_amount_Sql4)

    query5 = f'''
    select states,district_name,
    sum(appopens) as total_appopens
    from {sql}
    group by states, district_name
    order by total_appopens
    limit 10;'''

    Mycursor.execute(query5)
    sql5 = Mycursor.fetchall()
    connection.commit()

    DFQ5 = pd.DataFrame(sql5, columns=("States", "District", "AppOpens"))
    DFQ5.index = range(1, len(DFQ5) + 1)

    presentation_amount_Sql5 = py.bar(DFQ5, x="District", y="AppOpens",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title="Bottom 10 AppOpens from all States")
    st.plotly_chart(presentation_amount_Sql5)

    query6 = f'''
    select states,district_name,
    avg(appopens) as avg_appopens
    from {sql}
    group by states, district_name;'''

    Mycursor.execute(query6)
    sql6 = Mycursor.fetchall()
    connection.commit()

    DFQ6 = pd.DataFrame(sql6, columns=("States", "District", "AppOpens"))
    DFQ6.index = range(1, len(DFQ6) + 1)

    presentation_amount_Sql6 = py.bar(DFQ6, x="District", y="AppOpens",
                                       hover_name="States", color_discrete_sequence=['purple'],
                                       title="Average AppOpens from all States")
    st.plotly_chart(presentation_amount_Sql6)
    Mycursor.close()
    connection.close()


def get_payment_modes_counts(sql,state):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )
    Mycursor = connection.cursor()
    query = f'''
    select states, 
    sum(counts) as count,
    payment_names
    from {sql}
    where states = '{state}'
    group by states,payment_names
    order by states
    '''
    Mycursor.execute(query)
    data = Mycursor.fetchall()
    

    DFQ3 = pd.DataFrame(data, columns= ("States", "count", "payment_names"))
    DFQ3.index = range(1,len(DFQ3) + 1)

    presentation_payment = py.bar(DFQ3, x="payment_names", y="count", color_discrete_sequence=['purple'], title=f"Average Transaction Amounts from {sql}")
    st.plotly_chart(presentation_payment)



def get_Brands_counts(sql,state):
    connection = psycopg2.connect(
        database="PhonePe",
        user="postgres",
        password="kk07ch30",
        host="localhost",
        port="5432"
    )
    Mycursor = connection.cursor()
    query = f'''
    select states,
    sum(counts) as count,
    brands
    from aggregated_user
    group by states, brands
    order by states
    '''
    Mycursor.execute(query)
    data = Mycursor.fetchall()
    

    DFQ3 = pd.DataFrame(data, columns= ("States", "count", "brands"))
    # DFQ3.index = range(1,len(DFQ3) + 1)

    presentation_payment = py.bar(DFQ3, x="brands", y="count", color_discrete_sequence=['purple'], title=f"Average Transaction Amounts from {sql}")
    st.plotly_chart(presentation_payment)    






#Streamlit Title
st.markdown(
    "<h1 style='text-align: center; color: red; '>PHONEPE DATA VISUALIZATION</h1>",
    unsafe_allow_html=True
)

# Sidebar options
options = st.sidebar.selectbox(
    'Navigation',
    ['Home', 'Data', 'Charts']
)


if options == 'Home':
     pass

elif options == 'Data':
        column_names = ['Aggregated', 'Map', 'Top']
        selected_column  = st.selectbox("Select any of the following to display", ["Aggregated", "Map", "Top"])

        if selected_column == 'Aggregated':
            sub_option = st.radio("Select an Analysis to View", ['Insurance', 'Transaction', 'User'])
            
            if sub_option == "Insurance":
                 Year = st.slider("Select The Required Year", A_I_DF["Years"].min(), A_I_DF["Years"].max(), A_I_DF["Years"].min())
                 result_DF_Y= DataFrame_S_C_A_by_year(A_I_DF, Year)

                 Quarters = st.slider("Select The Required Quarter", result_DF_Y["Quarter"].min(), result_DF_Y["Quarter"].max(), result_DF_Y["Quarter"].min())
                 DataFrame_S_C_A_by_Quarter(result_DF_Y, Quarters)
            
            elif sub_option == "Transaction":
                 Year = st.slider("Select The Required Year", A_T_DF["Years"].min(), A_T_DF["Years"].max(), A_T_DF["Years"].min())
                 result_DF_Y2 = DataFrame_S_C_A_by_year(A_T_DF, Year)

                 Quarters = st.slider("Select The Required Quarter", result_DF_Y2["Quarter"].min(), result_DF_Y2["Quarter"].max(), result_DF_Y2["Quarter"].min())
                 result_DF_Y2_Q = DataFrame_S_C_A_by_Quarter(result_DF_Y2, Quarters)
                 states_info_Q = st.selectbox("Select The Required State To View In Quarter", result_DF_Y2_Q["States"].unique())
                 DataFrame_S_C_A_by_year2(result_DF_Y2_Q, states_info_Q)

                 States_info = st.selectbox("Select Any of the State to View", result_DF_Y2["States"].unique())
                 DataFrame_S_C_A_by_year2(result_DF_Y2, States_info)


            elif sub_option == "User":
                 Year = st.slider("Select The Required Year", A_U_DF["Years"].min(), A_U_DF["Years"].max(), A_U_DF["Years"].min())
                 result_DF_Y3 = DataFrame_S_C_A_by_year3(A_U_DF, Year)


                 Quarters = st.slider("Select The Required Quarter", result_DF_Y3["Quarter"].min(), result_DF_Y3["Quarter"].max(), result_DF_Y3["Quarter"].min())
                 result_DF_Y3_Q = DataFrame_S_C_A_by_Quarter3(result_DF_Y3, Quarters)


                 States_info = st.selectbox("Select Any of the State to View", result_DF_Y3_Q["States"].unique())
                 DataFrame_S_C_A_by_year3_State(result_DF_Y3_Q, States_info)



        elif selected_column == 'Map':
            sub_option = st.radio("Select an Analysis to View", ['Insurance', 'Transaction', 'User'])
            
            if sub_option == "Insurance":
                 Year4 = st.slider("Select The Required Year", M_I_DF["Years"].min(), M_I_DF["Years"].max(), M_I_DF["Years"].min())
                 result_DF_Y4 = DataFrame_S_C_A_by_year(M_I_DF, Year4)

                 states_info_Q4 = st.selectbox("Select The Required State To View In Quarter", result_DF_Y4["States"].unique())
                 DataFrame_D_C_A_by_year4(result_DF_Y4, states_info_Q4)

                 Quarters = st.slider("Select The Required Quarter", result_DF_Y4["Quarter"].min(), result_DF_Y4["Quarter"].max(), result_DF_Y4["Quarter"].min())
                 result_DF_Y4_Q = DataFrame_S_C_A_by_Quarter(result_DF_Y4, Quarters)

                 states_info_D4 = st.selectbox("Select The Required State To View In Quarter", result_DF_Y4_Q["States"].unique())
                 DataFrame_D_C_A_by_year4(result_DF_Y4_Q, states_info_D4)
            
            elif sub_option == "Transaction":
                 Year5 = st.slider("Select The Required Year", M_T_DF["Years"].min(), M_T_DF["Years"].max(), M_T_DF["Years"].min())
                 result_DF_Y5 = DataFrame_S_C_A_by_year(M_T_DF, Year5)

                 states_info_Q5 = st.selectbox("Select The Required State To View In Quarter", result_DF_Y5["States"].unique())
                 DataFrame_D_C_A_by_year4(result_DF_Y5, states_info_Q5)

                 Quarters = st.slider("Select The Required Quarter", result_DF_Y5["Quarter"].min(), result_DF_Y5["Quarter"].max(), result_DF_Y5["Quarter"].min())
                 result_DF_Y5_Q = DataFrame_S_C_A_by_Quarter(result_DF_Y5, Quarters)

                 states_info_D5 = st.selectbox("Select The State To View In Quarter", result_DF_Y5_Q["States"].unique())
                 DataFrame_D_C_A_by_year4(result_DF_Y5_Q, states_info_D5)
            
            elif sub_option == "User":
                 Year6 = st.slider("Select The Required Year You Want To View", M_U_DF["Years"].min(), M_U_DF["Years"].max(), M_U_DF["Years"].min())
                 result_DF_Y6 = DataFrame_S_C_A_by_year6(M_U_DF, Year6)


                 Quarters = st.slider("Select The Required Quarter", result_DF_Y6["Quarter"].min(), result_DF_Y6["Quarter"].max(), result_DF_Y6["Quarter"].min())
                 result_DF_Y6_Q = DataFrame_S_C_A_by_Q6(result_DF_Y6, Quarters)


                 states_info_D6 = st.selectbox("Select The State To View In Quarter", result_DF_Y6_Q["States"].unique())
                 DataFrame_D_C_A_by_state6(result_DF_Y6_Q, states_info_D6)


        elif selected_column == 'Top':
            sub_option = st.radio("Select an Analysis to View", ['Insurance', 'Transaction', 'User'])
            
            if sub_option == "Insurance":
                 Year7 = st.slider("Select The Required Year", T_I_DF["Years"].min(), T_I_DF["Years"].max(), T_I_DF["Years"].min())
                 result_DF_Y7 = DataFrame_S_C_A_by_year(T_I_DF, Year7)

                 states_info7 = st.selectbox("Select The State To View", result_DF_Y7["States"].unique())
                 DataFrame_S_C_A_by_State7(result_DF_Y7, states_info7)


                 Quarters = st.slider("Select The Required Quarter", result_DF_Y7["Quarter"].min(), result_DF_Y7["Quarter"].max(), result_DF_Y7["Quarter"].min())
                 result_DF_Y7_Q = DataFrame_S_C_A_by_Quarter(result_DF_Y7, Quarters)


            
            elif sub_option == "Transaction":
                 Year8 = st.slider("Select The Required Year", T_T_DF["Years"].min(), T_T_DF["Years"].max(), T_T_DF["Years"].min())
                 result_DF_Y8 = DataFrame_S_C_A_by_year(T_T_DF, Year8)

                 states_info8 = st.selectbox("Select The State To View", result_DF_Y8["States"].unique())
                 DataFrame_S_C_A_by_State7(result_DF_Y8, states_info8)


                 Quarters = st.slider("Select The Required Quarter", result_DF_Y8["Quarter"].min(), result_DF_Y8["Quarter"].max(), result_DF_Y8["Quarter"].min())
                 result_DF_Y8_Q = DataFrame_S_C_A_by_Quarter(result_DF_Y8, Quarters)
            
            elif sub_option == "User":
                 Year9 = st.slider("Select The Required Year", T_U_DF["Years"].min(), T_U_DF["Years"].max(), T_U_DF["Years"].min())
                 result_DF_Y9 = DataFrame_S_C_A_by_Year9(T_U_DF, Year9)


                 states_info9 = st.selectbox("Select The State To View", result_DF_Y9["States"].unique())
                 DataFrame_S_C_A_by_S9(result_DF_Y9, states_info9)

elif options == "Charts":
    Question = st.selectbox("Select any of the Question".upper(),
                 ["1. What are the Top 5 Transaction Amounts from all the Tables",
                 "2. What are the Bottom 5 Transaction Amounts from all the Tables",
                 "3. What are the Average Transaction Amounts from all the Tables",
                 "4. What are the Top 5 Transaction Counts from all the Tables",
                 "5. What are the Bottom 5 Transaction Counts from all the Tables",
                 "6. What are the Average Transaction Counts from all the Tables",
                 "7. What are the top & bottom registeredusers from Map users",
                 "8. What are the top & bottom Appopens from Map users",
                 "9. How much Count Made in Multiple Payment Modes from Aggregated Transaction",
                 "10. How much Count Made in Each Brands from Aggregated User"])
    
    if Question == "1. What are the Top 5 Transaction Amounts from all the Tables":
         st.subheader("AGGREGATED INSURANCE")
         Get_TopChart_Amounts("aggregated_insurance")
         st.subheader("AGGREGATED TRANSACTION")
         Get_TopChart_Amounts("aggregated_transaction")
         
         st.subheader("MAP INSURANCE")
         Get_TopChart_Amounts("map_insurance")
         st.subheader("MAP TRANSACTION")
         Get_TopChart_Amounts("map_transaction")
         
         st.subheader("TOP INSURANCE")
         Get_TopChart_Amounts("top_insurance")
         st.subheader("TOP TRANSACTION")
         Get_TopChart_Amounts("top_transaction")

    elif Question == "2. What are the Bottom 5 Transaction Amounts from all the Tables":
         st.subheader("AGGREGATED INSURANCE")
         Get_BottomChart_Amounts("aggregated_insurance")
         st.subheader("AGGREGATED TRANSACTION")
         Get_BottomChart_Amounts("aggregated_transaction")
         
         st.subheader("MAP INSURANCE")
         Get_BottomChart_Amounts("map_insurance")
         st.subheader("MAP TRANSACTION")
         Get_BottomChart_Amounts("map_transaction")
         
         st.subheader("TOP INSURANCE")
         Get_BottomChart_Amounts("top_insurance")
         st.subheader("TOP TRANSACTION")
         Get_BottomChart_Amounts("top_transaction")


    elif Question == "3. What are the Average Transaction Amounts from all the Tables":
         st.subheader("AGGREGATED INSURANCE")
         Get_AVGChart_Amounts("aggregated_insurance")
         st.subheader("AGGREGATED TRANSACTION")
         Get_AVGChart_Amounts("aggregated_transaction")
         
         st.subheader("MAP INSURANCE")
         Get_AVGChart_Amounts("map_insurance")
         st.subheader("MAP TRANSACTION")
         Get_AVGChart_Amounts("map_transaction")
         
         st.subheader("TOP INSURANCE")
         Get_AVGChart_Amounts("top_insurance")
         st.subheader("TOP TRANSACTION")
         Get_AVGChart_Amounts("top_transaction")


    elif Question == "4. What are the Top 5 Transaction Counts from all the Tables":
         st.subheader("AGGREGATED INSURANCE")
         Get_TopChart_Counts("aggregated_insurance")
         st.subheader("AGGREGATED TRANSACTION")
         Get_TopChart_Counts("aggregated_transaction")
         
         st.subheader("MAP INSURANCE")
         Get_TopChart_Counts("map_insurance")
         st.subheader("MAP TRANSACTION")
         Get_TopChart_Counts("map_transaction")
         
         st.subheader("TOP INSURANCE")
         Get_TopChart_Counts("top_insurance")
         st.subheader("TOP TRANSACTION")
         Get_TopChart_Counts("top_transaction")


    elif Question == "5. What are the Bottom 5 Transaction Counts from all the Tables":
         st.subheader("AGGREGATED INSURANCE")
         Get_BottomChart_Counts("aggregated_insurance")
         st.subheader("AGGREGATED TRANSACTION")
         Get_BottomChart_Counts("aggregated_transaction")
         
         st.subheader("MAP INSURANCE")
         Get_BottomChart_Counts("map_insurance")
         st.subheader("MAP TRANSACTION")
         Get_BottomChart_Counts("map_transaction")
         
         st.subheader("TOP INSURANCE")
         Get_BottomChart_Counts("top_insurance")
         st.subheader("TOP TRANSACTION")
         Get_BottomChart_Counts("top_transaction")


    elif Question == "6. What are the Average Transaction Counts from all the Tables":
         st.subheader("AGGREGATED INSURANCE")
         Get_AVGChart_Counts("aggregated_insurance")
         st.subheader("AGGREGATED TRANSACTION")
         Get_AVGChart_Counts("aggregated_transaction")
         
         st.subheader("MAP INSURANCE")
         Get_AVGChart_Counts("map_insurance")
         st.subheader("MAP TRANSACTION")
         Get_AVGChart_Counts("map_transaction")
         
         st.subheader("TOP INSURANCE")
         Get_AVGChart_Counts("top_insurance")
         st.subheader("TOP TRANSACTION")
         Get_AVGChart_Counts("top_transaction")


    elif Question == "7. What are the top & bottom registeredusers from Map users":
         states = st.selectbox("Select any of the state", M_U_DF["States"].unique())
         st.subheader("MAP USER")
         Get_ALLChart_Registers("map_user", states)


    elif Question == "8. What are the top & bottom Appopens from Map users":
         states = st.selectbox("Select any of the state", M_U_DF["States"].unique())
         st.subheader("MAP USER")
         Get_ALLChart_Appopens("map_user", states)


    elif Question == "9. How much Count Made in Multiple Payment Modes from Aggregated Transaction":
         states = st.selectbox("Select any of the state To View Its Counts From Payment Modes".upper(), A_T_DF["States"].unique())
         st.subheader("AGGREGATED TRANSACTION")
         st.write(states.upper())
         get_payment_modes_counts("aggregated_transaction",states)   



    elif Question == "10. How much Count Made in Each Brands from Aggregated User":
         states = st.selectbox("Select any of the state To View Its Counts From Brands".upper(), A_U_DF["States"].unique())
         st.subheader("AGGREGATED USER")
         st.write(states.upper())
         get_Brands_counts("aggregated_user",states)       